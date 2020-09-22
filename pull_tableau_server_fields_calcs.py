import pandas as pd
import sys, os, urllib
import requests, json
import certifi # For some certificates that aren't trusted by default
from pandas import json_normalize


def get_output_schema():
	return pd.DataFrame({
		'id':prep_string(),
		'name':prep_string(),
		'formula':prep_string(),
		'role':prep_string(),
		'dataCategory':prep_string(),
		'dataType':prep_string(),
		'sheet.id':prep_string(),
		'sheet.name':prep_string(),
		'sheet.workbook.id':prep_string(),
		'sheet.workbook.name':prep_string(),
		'sheet.workbook.owner.id':prep_string(),
		'sheet.workbook.owner.username':prep_string(),
		'sheet.workbook.owner.name':prep_string(),
		'sheet.workbook.owner.email':prep_string()
		# Etc, etc.
	})

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
						id,
						name,
						workbook {
							id,
							name,
        			        owner {
								id,
                  				username,
                  				name,
								email
			                }
						}
					}
				}
			}
		}
	}
		""" }

def get_calculations(input):
	
	# Server specifications, certificates and credentials
	server_base_url = input.iloc[0]["server_url"]
	server_api_version = input.iloc[0]["api_version"]
	server_rest_url = server_base_url + "/api/" + server_api_version
	server_metadata_url = server_base_url + "/api/metadata/graphql"
	server_certificates = input.iloc[0]["server_certificates"] # In case we need to trust additional CAs or specific certificates
	personal_access_token_name = input.iloc[0]["personal_access_token_name"]
	personal_access_token_secret = input.iloc[0]["personal_access_token_secret"]

	# If we need to use certificates, we'll specify that here
	if server_certificates is not None:
		with open("temp/temp_ca.pem", 'w') as outfile:
			outfile.write(server_certificates)
		server_certificates_file = "temp/temp_ca.pem" # We'll pass this to the next function (get_stuff) which will specify that when using requests
	else:
		server_certificates_file = certifi.where() # Need to pass the default certificates otherwise
	
	print("Server: \"" + server_base_url + "\"")
	
	##### Sign in through the REST API first, to the default site #####
	# Which we do to obtain an access token

	print("Signing in through REST API")

	request_url = server_rest_url + "/auth/signin"
	print("URL for sign-in: \"" + request_url + "\"")
	payload = { "credentials": {"personalAccessTokenName": personal_access_token_name, "personalAccessTokenSecret": personal_access_token_secret, "site": { "contentUrl": "" }}}
	headers = { "accept": "application/json", "content-type": "application/json" }
	r = requests.post(request_url, json=payload, headers=headers, verify=server_certificates_file)
	r.raise_for_status()
	
	# Get the token!
	r_json = json.loads(r.content)
	token = r_json["credentials"]["token"]
	# site_id = r_json["credentials"]["site"]["id"]
	
	# Get list of sites
	# Not in use for Metadata pull yet

	request_url = server_rest_url + "/sites"
	headers = { "accept": "application/json", "content-type": "application/json", "X-Tableau-Auth" : token }
	r = requests.get(request_url, headers=headers, verify=server_certificates_file)
	r.raise_for_status()

	r_json = json.loads(r.content)

	for site in r_json["sites"]["site"]:

		print("Processing site " + site["name"])

		##### Perform Metadata API call to get data #####

		request_url = server_rest_url + "/auth/switchSite"
		payload = { "site": { "contentUrl": site["contentUrl"] }}
		headers = { "accept": "application/json", "content-type": "application/json", "X-Tableau-Auth" : token }
		try:
			r = requests.post(request_url, json=payload, headers=headers, verify=server_certificates_file)
			r.raise_for_status()
			r_json = json.loads(r.content)
			token = r_json["credentials"]["token"]
			headers = { "accept": "application/json", "content-type": "application/json", "X-Tableau-Auth" : token }
		except requests.exceptions.HTTPError:
			"Staying in default site..."

		request_url = server_metadata_url
		r = requests.post(request_url, json=content_type_payload, headers=headers, verify=server_certificates_file)
		r.raise_for_status()
		r_json = json.loads(r.content)
		
		if len(r_json["data"]["calculatedFieldsConnection"]["nodes"]) == 0:
			# We "try" this part because some sites may have no content
			print(r_json)
			print("Parsing response failed; maybe nothing was returned for this site?")
		else:
			# The reference to ["nodes"] below is necessary for content_types that refer to "connections" such as "calculatedFieldConnections"
			# It should be dropped for the ones where that does not apply, such as "calculatedFields"
			# We'll normalize for each level we have in here
			print("Response received for this site, and it has calculated fields")
			# Calculations
			df_calculations = json_normalize(r_json["data"]["calculatedFieldsConnection"]["nodes"], meta=None, record_path=None, record_prefix="field.") # Not _really_ normalization but it works, and we do that next anyway
			print(df_calculations)
			# Explode Sheets
			df_calculations = df_calculations.explode(column="sheetsConnection.nodes").reset_index(drop=True)
			# Fill the rows with no sheet information first
			# According to: https://stackoverflow.com/questions/44050853/pandas-json-normalize-and-null-values-in-json
			df_calculations["sheetsConnection.nodes"] = df_calculations["sheetsConnection.nodes"].apply(lambda x: {} if pd.isna(x) else x)
			print(df_calculations)
			# "Select" Sheets properties
			try: 
				df_calculations_sheets = df_calculations.join(json_normalize(df_calculations["sheetsConnection.nodes"].tolist()).add_prefix("sheet.")).drop("sheetsConnection.nodes", axis=1)
			except AttributeError as e:
				print(e)
				print("This calculation is probably not used anywhere. Or something.")
				df_calculations_sheets = df_calculations.drop("sheetsConnection.nodes", axis=1) # Just keep this "as is", in that case
			else:
				print(df_calculations_sheets)

			##### Append results into one dataframe #####
			try:
				print(data)
			except UnboundLocalError as e:
				print("Dataframe does not exist yet, creating")
				data = df_calculations_sheets # create with the same model
				# print(data)
			else:
				data = pd.concat([data, df_calculations_sheets]) # concat
				# print(data)
		

	##### We're done #####
	
	print("Signing out")
	request_url = server_base_url + "/auth/signout"
	r = requests.post(request_url, headers = headers, verify=server_certificates_file)
	
	print("Columns:")
	print(list(data.columns.values))
	print("Fully done.")
	return data