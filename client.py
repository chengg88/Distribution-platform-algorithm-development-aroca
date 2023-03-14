import requests
import json

# response = requests.post('http://127.0.0.1:8080/api/addBatchNo', data=json.dumps({"batchNo":"202208251335"}))
response = requests.post('http://10.71.253.134:8080/api/addBatchNo', data=json.dumps({"batchNo":"546456"}))
# hdr = {"Content-Type": "application/json"}
# response = requests.post('http://10.71.253.133:8080/api/calcBatchNoDone', data=json.dumps({"sysId": "ALGORITHM","batchNo": "202208251335"}), headers=hdr)
print(response)
print(response.text)


# 202208251335