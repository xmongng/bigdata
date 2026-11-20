#!/usr/bin/env python3
"""Provision the vn-news Iceberg REST catalog for Apache Polaris 1.8.0.

Run on VM1, with POLARIS_ADMIN_SECRET sourced from ~/vn-news-core/.env.
Creates catalog, roles/grant, and one service principal (its credentials are
written to client.env exactly once, never printed).
"""
import json
import os
import pathlib
import sys
import urllib.error
import urllib.parse
import urllib.request

BASE = 'http://192.168.1.195:8181'
CATALOG = 'news_lake'
PRINCIPAL = 'news_pipeline_client'
PRINCIPAL_ROLE = 'news_pipeline_role'
CATALOG_ROLE = 'news_content_role'
WAREHOUSE = 's3://vn-news-lake/iceberg_warehouse/'
CLIENT_ENV = pathlib.Path('client.env')


def api(method, path, token=None, payload=None, form=False, accept_conflict=False):
    headers = {'Polaris-Realm': 'POLARIS'}
    data = None
    if payload is not None:
        if form:
            data = urllib.parse.urlencode(payload).encode('utf-8')
            headers['Content-Type'] = 'application/x-www-form-urlencoded'
        else:
            data = json.dumps(payload).encode('utf-8')
            headers['Content-Type'] = 'application/json'
    if token:
        headers['Authorization'] = 'Bearer ' + token
    request = urllib.request.Request(BASE + path, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            raw = response.read()
            parsed = json.loads(raw) if raw.strip() else {}
            return response.status, parsed
    except urllib.error.HTTPError as error:
        if accept_conflict and error.code == 409:
            return 409, {}
        detail = error.read().decode('utf-8', errors='replace')[:1000]
        print('HTTP_ERROR', method, path, 'status=', error.code, 'response=', detail, file=sys.stderr)
        raise SystemExit(1)
    except urllib.error.URLError as error:
        print('CONNECTION_ERROR', method, path, str(error.reason), file=sys.stderr)
        raise SystemExit(1)


def create_or_existing(token, label, path, payload):
    status, result = api('POST', path, token, payload, accept_conflict=True)
    if status == 409:
        print(label, 'ALREADY_EXISTS')
    else:
        print(label, 'CREATED', status)
    return status, result


def main():
    secret = os.environ.get('POLARIS_ADMIN_SECRET')
    if not secret:
        print('ERROR: Source .env on VM1 before running: set -a; . ./.env; set +a', file=sys.stderr)
        raise SystemExit(1)
    _, auth = api('POST', '/api/catalog/v1/oauth/tokens', payload={
        'grant_type': 'client_credentials',
        'client_id': 'root',
        'client_secret': secret,
        'scope': 'PRINCIPAL_ROLE:ALL',
    }, form=True)
    token = auth['access_token']
    print('ROOT_AUTH_OK (token not displayed)')

    catalog_payload = {'catalog': {
        'name': CATALOG,
        'type': 'INTERNAL',
        'properties': {'default-base-location': WAREHOUSE},
        'storageConfigInfo': {
            'storageType': 'S3',
            'allowedLocations': [WAREHOUSE],
            'endpoint': 'http://192.168.1.195:9000',
            'endpointInternal': 'http://minio:9000',
            'pathStyleAccess': True,
            'stsUnavailable': True,
            'kmsUnavailable': True,
            'region': 'us-east-1',
        },
    }}
    create_or_existing(token, 'CATALOG', '/api/management/v1/catalogs', catalog_payload)
    _, existing = api('GET', '/api/management/v1/catalogs/' + CATALOG, token)
    cat = existing.get('catalog', existing)
    assert cat.get('properties', {}).get('default-base-location') == WAREHOUSE, \
        'Existing catalog warehouse differs; do not overwrite it'
    assert cat.get('storageConfigInfo', {}).get('storageType') == 'S3', \
        'Existing catalog storage type differs'
    print('CATALOG_VERIFIED', CATALOG)

    create_or_existing(token, 'PRINCIPAL_ROLE', '/api/management/v1/principal-roles',
                       {'principalRole': {'name': PRINCIPAL_ROLE}})
    create_or_existing(token, 'CATALOG_ROLE',
                       '/api/management/v1/catalogs/' + CATALOG + '/catalog-roles',
                       {'catalogRole': {'name': CATALOG_ROLE}})

    api('PUT', '/api/management/v1/catalogs/' + CATALOG + '/catalog-roles/' +
        CATALOG_ROLE + '/grants', token,
        {'grant': {'type': 'catalog', 'privilege': 'CATALOG_MANAGE_CONTENT'}})
    print('CATALOG_GRANT_OK')

    api('PUT', '/api/management/v1/principal-roles/' + PRINCIPAL_ROLE +
        '/catalog-roles/' + CATALOG, token,
        {'catalogRole': {'name': CATALOG_ROLE}})
    print('CATALOG_ROLE_ASSIGNED')

    if CLIENT_ENV.exists():
        if CLIENT_ENV.stat().st_size == 0:
            raise SystemExit('ERROR: client.env exists but is empty. Inspect manually.')
        print('CLIENT_ENV_ALREADY_EXISTS (will not overwrite)')
    else:
        # Deliberately create the principal AFTER the catalog and roles, to
        # minimize the risk of losing its one-time credentials on partial failure.
        status, result = api('POST', '/api/management/v1/principals', token,
                             {'principal': {'name': PRINCIPAL, 'properties': {}}},
                             accept_conflict=True)
        if status == 409:
            raise SystemExit('ERROR: principal exists but client.env is missing. '
                             'Credentials are only returned once; recover/rotate via administrator.')
        credentials = result['credentials']
        old_umask = os.umask(0o077)
        try:
            with CLIENT_ENV.open('x', encoding='utf-8') as dest:
                dest.write('POLARIS_CLIENT_ID=' + credentials['clientId'] + '\n')
                dest.write('POLARIS_CLIENT_SECRET=' + credentials['clientSecret'] + '\n')
            os.chmod(CLIENT_ENV, 0o600)
        finally:
            os.umask(old_umask)
        print('CLIENT_CREDENTIALS_SAVED', str(CLIENT_ENV))

    api('PUT', '/api/management/v1/principals/' + PRINCIPAL + '/principal-roles', token,
        {'principalRole': {'name': PRINCIPAL_ROLE}})
    print('PRINCIPAL_ROLE_ASSIGNED')

    # An unauthenticated catalog configuration request produces 401. This one
    # uses a service-client token read from the file created above.
    client_values = {}
    with CLIENT_ENV.open(encoding='utf-8') as saved:
        for line in saved:
            key, sep, value = line.rstrip('\n').partition('=')
            if sep:
                client_values[key] = value
    _, client_auth = api('POST', '/api/catalog/v1/oauth/tokens', payload={
        'grant_type': 'client_credentials',
        'client_id': client_values['POLARIS_CLIENT_ID'],
        'client_secret': client_values['POLARIS_CLIENT_SECRET'],
        'scope': 'PRINCIPAL_ROLE:ALL',
    }, form=True)
    api('GET', '/api/catalog/v1/config?warehouse=' + urllib.parse.quote(CATALOG),
        token=client_auth['access_token'])
    print('CLIENT_CATALOG_CONFIG_OK')
    print('A7_COMPLETE: catalog=news_lake; client.env created on VM1')


if __name__ == '__main__':
    main()
