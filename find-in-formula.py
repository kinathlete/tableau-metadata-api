import sys, getopt, urllib
import requests, json
# import graphene	

def main(argv):
	
	##### Prepare and parse args #####
	
	server = ""
	site = ""
	username = ""
	password = ""
	search_term = ""
	out_file = ""
	
	args_usage = "tableau-server-connections-updater.py -i <inputfile> -s <server> -u <username> -p <password> -t <search-term>"
	
	try:
		opts, args = getopt.getopt(argv,"hs:t:u:p:q:o:", ["server=", "site=", "username=", "password=", "query=", "out-file="])
	except getopt.GetoptError:
		print args_usage
		sys.exit(2)
	for opt, arg in opts:
		if opt == '-h':
			print args_usage
			sys.exit()
		elif opt in ("-s", "--server"):
			server = arg
		elif opt in ("-t", "--site"):
			site = arg
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
	
	print "Search term: \"" + search_term + "\""
	print "Server: \"" + server + "\""
			
	##### Sign in through the REST API first #####
	# Which we do to obtain an access token

	print "Signing in through REST API"

	request_url = server_rest_url + "/auth/signin"
	payload = { "credentials": {"name": username, "password": password, "site": { "contentUrl": site }}}
	headers = { "accept": "application/json", "content-type": "application/json" }
	r = requests.post(request_url, json=payload, headers=headers)
	r.raise_for_status()
	
	# Get the token!
	r_json = json.loads(r.content)
	token = r_json["credentials"]["token"]
	# site_id = r_json["credentials"]["site"]["id"]
	headers['X-Tableau-Auth'] = token
	
	##### Perform Metadata API call to get data #####

	request_url = server_metadata_url
	payload = { "query" : """
	query calculatedFields {
		calculatedFieldsConnection {
			nodes {
				id,
				name,
				formula,
				role,
				dataCategory,
				sheetsConnection {
					nodes {
						name,
						workbook {
							name
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

	# print r.content

	##### Find fields using search term #####

	# Prepare file
	f = open(out_file, "w+")
	f.write("field_name|formula|workbook")
	f.close()
	f = open(out_file, "a")

	for calculatedField in r_json["data"]["calculatedFieldsConnection"]["nodes"]:
		if str(calculatedField["formula"]).find(search_term) >= 0:
			try:
				found_result = calculatedField["name"] + "|\"" + calculatedField["formula"].replace("\r", "\\n").replace("\n", "") + "\"|" + calculatedField["sheetsConnection"]["nodes"][0]["workbook"]["name"] + "\n"
			except IndexError:
				found_result = calculatedField["name"] + "|\"" + calculatedField["formula"].replace("\r", "\\n").replace("\n", "") + "\"|\n"
			print found_result
			f.write(found_result)



	##### We're done #####
	
	print "Signing out"
	request_url = server_base_url + "/auth/signout"
	r = requests.post(request_url, headers = headers)

if __name__ == "__main__":
   main(sys.argv[1:])