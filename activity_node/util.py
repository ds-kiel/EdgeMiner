# Copyright 2024 Kiel University
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import requests

def contact_another_server(srv_ip, URI, req='POST', data=None, params=None, json=None, timeout_s=1):
    # Try to contact another serverthrough a POST or GET
    # usage: server.contact_another_server("10.1.1.1", "/index", "POST", data)
    success = False
    try:
        if 'POST' in req:
            # We handle data string as json for now
            headers = {'Content-type': 'application/json', 'Accept': 'application/json'}
            res = requests.post('http://{}{}'.format(srv_ip, URI), data=data, json=json, headers=headers, timeout=timeout_s)
        elif 'GET' in req:
            headers = {'Accept': 'application/json'}
            res = requests.get(f"http://{srv_ip}{URI}", headers=headers, params=params)
            # result can be accessed res.json()
        if res.status_code == 200:
            success = True

    except Exception as e:
        print("[ERROR] "+str(e))
        res = None

    return (success, res)
