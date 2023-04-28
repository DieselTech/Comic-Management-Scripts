import os
import requests

path = "M:\comics"  # Replace with the path to your comics || This is for if your running the script on a different machine than your kavita host. 
exclude_list = ["Marvel", "DC Comics", "Image"]  # Replace with your publisher names to exclude. Thsese are generally the biggest folders in the library and will take a long time to scan. 

url = "http://192.168.0.100:3888/api/Library/create" # Change with your own instances URL
jwt_token = "eyJhbGciOiJIUzUxMiIsInR5cCI6IkpXVCJ9.eyJuYW1lIjoiZGllc2VsIiwibmFtZWlkIjoiMSIsInJvbGUiOlsiQWRtaW4iLCJDaGFuZ2UgUGFzc3dvcmQiLCJDaGFuZ2UgUmVzdHJpY3Rpb24iLCJMb2dpbiJdLCJuYmYiOjE2ODE5NDg3OTQsImV4cCI6MTY4MzE1ODM5NCwiaWF0IjoxNjgxOTQ4Nzk0fQ.kSaT7tK3XPBCDN9R8v1hTSl1SP_AmWVEM7uWfwKyUWEZaG8lkhb_CSAr0jkpu0Pvvnzw9401mJJLX1a_BUaS_A"

# Get your own JWT token by going to dev tools in your web browser, opening up the 'Storage' tab and then go to local storage. The token can be tricky to copy and paste since it's crazy long.
# Make sure you don't include the refresh token or other parts that are unneeded. You can uncomment the line below if you need help seeing what is being sent. 

#print(jwt_token)


headers = {
    "Authorization": f"Bearer {jwt_token}",
    "Content-Type": "application/json"
}

# Line 25 where 'os.path.join' is for accounting for dockers inside container paths. It will put /comics in front of the folder name outputted by the path

for entry in os.scandir(path):
    if entry.is_dir() and entry.name not in exclude_list:
        full_path = os.path.join('/comics/', entry.name)
        payload = {
            "name": entry.name,
            "type": 1, # 0 = manga, 1 = comics, 2 = books
            "folders": [full_path]
         }
        response = requests.post(url, headers=headers, json=payload)
        print(f"Folder '{entry.name}' sent. Response: {response.status_code}")