import sys, os, urllib
import requests, json
import pandas as pd
from pandas.io.json import json_normalize

# We put this in /opt/TabPy-0.6.1/tabpy-server/tabpy_server/
# Oh and also in /opt/TabPy-0.6.1/tabpy-server/build/lib/tabpy_server/pull_tableau_server_metadata.py apparently

def get_stuff(server_base_url, username, password, content_type, content_type_payload, json_meta_path, json_record_path, json_record_prefix):

	server_rest_url = server_base_url + "/api/3.6"
	server_metadata_url = server_base_url + "/relationship-service-war/graphql"
	
	print("Server: \"" + server_base_url + "\"")
			
	##### Sign in through the REST API first, to the default site #####
	# Which we do to obtain an access token

	print("Signing in through REST API")

	request_url = server_rest_url + "/auth/signin"
	payload = { "credentials": {"username": username, "password": password, "site": { "contentUrl": "" }}}
	headers = { "accept": "application/json", "content-type": "application/json" }
	r = requests.post(request_url, json=payload, headers=headers)
	r.raise_for_status()
	
	# Get the token!
	r_json = json.loads(r.content)
	token = r_json["credentials"]["token"]
	# site_id = r_json["credentials"]["site"]["id"]

	with open('logging/metadata_json.txt', 'w') as outfile:
		json.dump(r_json, outfile)
	
	# Get list of sites
	# Not in use for Metadata pull yet

	request_url = server_rest_url + "/sites"
	headers = { "accept": "application/json", "content-type": "application/json", "X-Tableau-Auth" : token }
	r = requests.get(request_url, headers=headers)
	r.raise_for_status()

	r_json = json.loads(r.content)

	with open('logging/metadata_json.txt', 'w') as outfile:
			json.dump(r_json, outfile)

	for site in r_json["sites"]["site"]:

		print("Processing site " + site["name"])

		##### Perform Metadata API call to get data #####

		request_url = server_rest_url + "/auth/switchSite"
		payload = { "site": { "contentUrl": site["contentUrl"] }}
		headers = { "accept": "application/json", "content-type": "application/json", "X-Tableau-Auth" : token }
		try:
			r = requests.post(request_url, json=payload, headers=headers)
			r.raise_for_status()
			r_json = json.loads(r.content)
			token = r_json["credentials"]["token"]
			headers = { "accept": "application/json", "content-type": "application/json", "X-Tableau-Auth" : token }
		except requests.exceptions.HTTPError:
			"Staying in default site..."

		request_url = server_metadata_url
		r = requests.post(request_url, json=content_type_payload, headers=headers)
		r.raise_for_status()
		r_json = json.loads(r.content)

		with open('logging/metadata_json.txt', 'w') as outfile:
			json.dump(r_json, outfile)
		
		r_json_normalized = json_normalize(r_json["data"][content_type], meta=json_meta_path, record_path=json_record_path, record_prefix=json_record_prefix)

		##### Parse results into dataframe #####

		data = r_json_normalized
		data.to_csv("logging/metadata_csv.csv")

	##### We're done #####
	
	print("Signing out")
	request_url = server_base_url + "/auth/signout"
	r = requests.post(request_url, headers = headers)

	return data