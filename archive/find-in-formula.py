import sys, os, getopt, urllib
import requests, json
# Necessary for writing to file successfully
reload(sys)
sys.setdefaultencoding("utf-8")

def main(argv):
	
	##### Prepare and parse args #####
	
	server = ""
	username = ""
	password = ""
	search_term = ""
	out_file = ""
	
	args_usage = "field-in-formula.py -s <server> -u <username> -p <password> -q <search-term> -o <out-file>"
	
	try:
		opts, args = getopt.getopt(argv,"hs:u:p:q:o:", ["server=", "username=", "password=", "query=", "out-file="])
	except getopt.GetoptError:
		print args_usage
		sys.exit(2)
	for opt, arg in opts:
		if opt == '-h':
			print args_usage
			sys.exit()
		elif opt in ("-s", "--server"):
			server = arg
		elif opt in ("-u", "--username"):
			username = arg
		elif opt in ("-p", "--password"):
			password = arg
		elif opt in ("-q", "--query"):
			search_term = arg
		elif opt in ("-o", "--out-file"):
			out_file = arg

	server_base_url = "http://" + server
	server_rest_url = server_base_url + "/api/3.4"
	server_metadata_url = server_base_url + "/relationship-service-war/graphql"

	# Prepare file
	f = open(out_file, "w+")
	f.write("site|field_name|formula|workbook\n")
	f.close()
	f = open(out_file, "a")
	
	print "Search term: \"" + search_term + "\""
	print "Server: \"" + server + "\""
			
	##### Sign in through the REST API first, to the default site #####
	# Which we do to obtain an access token

	print "Signing in through REST API"

	request_url = server_rest_url + "/auth/signin"
	payload = { "credentials": {"name": username, "password": password, "site": { "contentUrl": "" }}}
	headers = { "accept": "application/json", "content-type": "application/json" }
	r = requests.post(request_url, json=payload, headers=headers)
	r.raise_for_status()
	
	# Get the token!
	r_json = json.loads(r.content)
	token = r_json["credentials"]["token"]
	# site_id = r_json["credentials"]["site"]["id"]
	
	# Get list of sites

	request_url = server_rest_url + "/sites"
	headers = { "accept": "application/json", "content-type": "application/json", "X-Tableau-Auth" : token }
	r = requests.get(request_url, headers=headers)
	r.raise_for_status()

	r_json = json.loads(r.content)

	for site in r_json["sites"]["site"]:

		print "Processing site " + site["name"]

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
		payload = { "query" : """
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
		""", "operationName" : "calculatedFields"}
		r = requests.post(request_url, json=payload, headers=headers)
		r.raise_for_status()
		r_json = json.loads(r.content)

		f_site = open("sites_output/" + site["name"] + ".json", "w+")
		f_site.write(r.content)
		f_site.close()

		##### Find fields using search term #####

		for calculatedField in r_json["data"]["calculatedFieldsConnection"]["nodes"]:
			if str(calculatedField["formula"]).find(search_term) >= 0:
				try:
					found_result = site["name"] + "|\"" + calculatedField["name"] + "\"|\"" + calculatedField["formula"].replace("\r", "\\n").replace("\n", "") + "\"|" + calculatedField["sheetsConnection"]["nodes"][0]["workbook"]["name"] + "\n"
				except IndexError:
					found_result = site["name"] + "|\"" + calculatedField["name"] + "\"|\"" + calculatedField["formula"].replace("\r", "\\n").replace("\n", "") + "\"|\n"
				print found_result.encode("utf-8")
				f.write(found_result)



	##### We're done #####
	
	print "Signing out"
	request_url = server_base_url + "/auth/signout"
	r = requests.post(request_url, headers = headers)

if __name__ == "__main__":
   main(sys.argv[1:])