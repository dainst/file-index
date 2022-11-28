# Creating a new password hash

You need a running instance of OpenSearch to create a password hash. See: https://opensearch.org/docs/1.3/security-plugin/configuration/yaml/#internal_usersyml

With this docker setup the command would be:

```
docker exec file-index-opensearch plugins/opensearch-security/tools/hash.sh -p <your new password>
```

When changing the internal_users.yml, restart the containers and make sure to adjust the [.env](../.env) file for the new password. Otherwise your python scripts will fail push data to the index.