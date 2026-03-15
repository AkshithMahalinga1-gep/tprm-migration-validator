with open('./token.txt', 'r') as f:
    token = f.read().strip()

if not token.lower().startswith('bearer '):
    token = 'Bearer ' + token

neo4j_api = {
    "url": "https://leoaks.gep.com/leo-storage-dataservice/api/v1/StorageService/Run",
    "Authorization": token
}