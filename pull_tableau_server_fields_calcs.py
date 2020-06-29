import pandas as pd
import sys, os, urllib
import requests, json
from pandas.io.json import json_normalize

def get_stuff(server_base_url, personal_access_token_name, personal_access_token_secret, content_type, content_type_payload, json_meta_path, json_record_path, json_record_prefix):

	server_rest_url = server_base_url + "/api/3.6"
	server_metadata_url = server_base_url + "/relationship-service-war/graphql"
	
	print("Server: \"" + server_base_url + "\"")
			
	##### Sign in through the REST API first, to the default site #####
	# Which we do to obtain an access token

	print("Signing in through REST API")

	request_url = server_rest_url + "/auth/signin"
	payload = { "credentials": {"personalAccessTokenName": personal_access_token_name, "personalAccessTokenSecret": personal_access_token_secret, "site": { "contentUrl": "" }}}
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

	data = pd.DataFrame({'id' : [], 'name' : [], 'isHidden' : [], 'description' : [], 'dataType' : [], 'aggregation' : [], 'formula' : [], 'downstreamWorkbookb' : []})

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
		
		r_json_normalized = json_normalize(r_json["data"][content_type], meta=json_meta_path, record_path=json_record_path, record_prefix=json_record_prefix)

		##### Parse results into dataframe #####

		data = pd.concat([data, r_json_normalized])

		with open('logging/metadata_' + site["contentUrl"] + '.json', 'w') as outfile:
			json.dump(r_json, outfile)
		data.to_csv("logging/metadata_" + site["contentUrl"] + ".csv", index=False)
		r_json_normalized.to_csv("logging/metadata_normalized_" + site["contentUrl"] + ".csv", index=False)

	##### We're done #####
	
	print("Signing out")
	request_url = server_base_url + "/auth/signout"
	r = requests.post(request_url, headers = headers)

	# some logging

	data.to_csv("logging/metadata_csv.csv", index=False)
	
	return data.head(n=5)

content_type_payload = { "query" : """
	query calculatedFields {
		calculatedFieldsConnection (first: 10000) {
			nodes {
				id,
				name,
				formula,
				role,
				dataCategory,
				dataType,
				sheetsConnection {
					nodes {
						name,
						workbook {
							name,
        			        owner {
                  				username,
                  				name
			                }
						}
					}
				}
			}
		}
	}
		""" }

def get_output_schema():
	return pd.DataFrame({
		'calculatedFieldsConnection.nodes.id':prep_string(),
		'calculatedFieldsConnection.nodes.name':prep_string(),
		'calculatedFieldsConnection.nodes.formula':prep_string(),
		'calculatedFieldsConnection.nodes.sheetsConnection.nodes.workbook.owner.username':prep_string()
		# Etc, etc.
	})

def get_things(df):
	
	server_base_url = df.iloc[0]["server_url"]
	personal_access_token_name = df.iloc[0]["personal_access_token_name"]
	personal_access_token_secret = df.iloc[0]["personal_access_token_secret"]

	data = get_stuff(server_base_url, personal_access_token_name, personal_access_token_secret, "calculatedFields", content_type_payload, None, None, "field.")
	return data