# Authentication

Authentication is an optional feature that can be enabled through [Route Dependencies](#route-dependencies).

## Route Dependencies

### Configuration

Route dependencies for endpoints can enable through the `STAC_FASTAPI_ROUTE_DEPENDENCIES` 
environment variable as a path to a JSON file or a JSON string.

#### Route Dependency

A Route Dependency must include `routes`, a list of at least one [Route](#routes), and `dependencies` a
list of at least one [Dependency](#dependencies).

#### Routes

A Route must include a `path`, the relative path to the endpoint, and a `method`, the request method of the path.

#### Dependencies

A Dependency must include the `method`, a dot seperated path to the [Dependency](https://fastapi.tiangolo.com/tutorial/dependencies), and 
can include any `args` or `kwargs` for the method.

#### Example
```
STAC_FASTAPI_ROUTE_DEPENDENCIES=[
  {
    "routes": [
      {
        "method": "GET",
        "path": "/collections"
      }
    ],
    "dependencies": [
      {
        "method": "fastapi.security.OAuth2PasswordBearer",
        "kwargs": {
          "tokenUrl": "token"
        }
      }
    ]
  }
]
```

## Examples

[docker-compose.route_dependencies.yml](docker-compose.route_dependencies.yml), [docker-compose.basic_auth.yml](docker-compose.basic_auth.yml), and [docker-compose.oauth2.yml](docker-compose.oauth2.yml)
 give example for 3 different authentication configurations.

### Route dependencies

[docker-compose.route_dependencies.yml](docker-compose.route_dependencies.yml) gives an example of 
the `STAC_FASTAPI_ROUTE_DEPENDENCIES` environment variable adding the `conftest.must_be_bob` route 
dependency to the `GET` method on `/collections` endpoint.

#### Configuration

```json
[
  {
    "routes": [
      {
        "method": "GET",
        "path": "/collections"
      }
    ],
    "dependencies": [
      {
        "method": "conftest.must_be_bob"
      }
    ]
  }
]
```

### Basic Auth

This example illustrates how to add the [Basic Auth](../../stac_fastapi/core/stac_fastapi/core/basic_auth.py) Route Denpendency 
which allows a list of `user` and `password` pairs to be used to protect the specified routes.
The example defines two users: an **admin** user with full permissions (*) and a **reader** user with 
limited permissions to specific read-only endpoints.

#### Configuration

```json
[
  {
    "routes": [
      {
        "method": "*",
        "path": "*"
      }
    ],
    "dependencies": [
      {
        "method": "stac_fastapi.core.basic_auth.BasicAuth",
        "kwargs": {
          "credentials":[
            {
              "username": "admin",
              "password": "admin"
            }
          ]
        }
      }
    ]
  },
  {
    "routes": [
      {"path": "/", "method": ["GET"]},
      {"path": "/conformance", "method": ["GET"]},
      {"path": "/collections/{collection_id}/items/{item_id}", "method": ["GET"]},
      {"path": "/search", "method": ["GET", "POST"]},
      {"path": "/collections", "method": ["GET"]},
      {"path": "/collections/{collection_id}", "method": ["GET"]},
      {"path": "/collections/{collection_id}/items", "method": ["GET"]},
      {"path": "/queryables", "method": ["GET"]},
      {"path": "/queryables/collections/{collection_id}/queryables", "method": ["GET"]},
      {"path": "/_mgmt/ping", "method": ["GET"]}
    ],
    "dependencies": [
      {
        "method": "stac_fastapi.core.basic_auth.BasicAuth",
        "kwargs": {
          "credentials":[
            {
              "username": "reader",
              "password": "reader"
            }
          ]
        }
      }
    ]
  }
]
```

### Oauth2

This example illustrates how the `STAC_FASTAPI_ROUTE_DEPENDENCIES` environment variable can be used to point to a JSON file.

The [FastAPI OAuth2PasswordBearer](../../stac_fastapi/core/stac_fastapi/core/basic_auth.py) Denpendency is applied to all routes 
and methods using the `*` wildcard. This dependeny follows the [Oauth 2.0 Password Grant](https://oauth.net/2/grant-types/password) flow.

The [Basic Auth](../../stac_fastapi/core/stac_fastapi/core/basic_auth.py) Denpendency is also applied to the `GET` method 
on `/collections` endpoint. To demonstate how multiple dependencies can be applied to one endpoint.


#### Keycloak

For the Oauth 2.0 flow [Keycloak](https://www.keycloak.org/) has been used as the identity provider, as it supports [OIDC](https://www.microsoft.com/en-us/security/business/security-101/what-is-openid-connect-oidc) (an extension to OAuth2).

In the Password Grant flow the user authenticates with the Keycloak server and recieves an authorization token. This token 
is then used by STAC FastAPI to verify (via the Keycloak server) the user's identity and permissions. [This article](https://darutk.medium.com/diagrams-of-all-the-openid-connect-flows-6968e3990660) 
gives a nice visual explanation of many of the OpenID connet flows.

The Keycloak server is prepopulated with a `STAC` realm with one user `bob` with the password `bobpass` as an example. [This article](https://wkrzywiec.medium.com/create-and-configure-keycloak-oauth-2-0-authorization-server-f75e2f6f6046)
gives the steps to set up a Keycloak server with Docker. And [this guide](https://www.keycloak.org/server/importExport) shows how to import 
and export realms.

#### Configuration

```json
[
  {
    "routes": [
      {
        "method": "*",
        "path": "*"
      }
    ],
    "dependencies": [
      {
        "method": "fastapi.security.OAuth2PasswordBearer",
        "kwargs": {
          "tokenUrl": "http://Keycloak:8083/auth/realms/stac/protocol/openid-connect/token"
        }
      }
    ]
  },
  {
    "routes": [
      {
        "path": "/collections/{collection_id}/items/{item_id}",
        "method": "GET"
      },
      {
        "path": "/search",
        "method": [
          "GET",
          "POST"
        ]
      },
      {
        "path": "/collections",
        "method": "GET"
      }
    ],
    "dependencies": [
      {
        "method": "stac_fastapi.core.basic_auth.BasicAuth",
        "kwargs": {
          "credentials": [
            {
              "username": "reader",
              "password": "reader"
            }
          ]
        }
      }
    ]
  }
]
```
