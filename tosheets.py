import httplib2
import sys
import re
import base64
from bs4 import BeautifulSoup
import gspread
import string

from apiclient.discovery import build
from apiclient import errors
from oauth2client.file import Storage
from oauth2client.client import AccessTokenRefreshError
from oauth2client.client import OAuth2WebServerFlow
from oauth2client.client import flow_from_clientsecrets
from oauth2client.tools import run
from gspread.exceptions import SpreadsheetNotFound

## USER INPUTS
##################################################################################################
# User inputs for the select_google_sheet to update --> the name of the spreadsheet
# as it appears in the user's google drive. The gmail label (message_label) 
# that currently applies to messages which have not been added the spreadsheet. 
# The new_message_label that will be applied to those same messages after they've 
# been parsed and updated in the spreadsheet. 

select_google_sheet = 'test'
message_label = ['Label_1']
new_message_label = ['Label_2']
##################################################################################################


CLIENT_SECRETS_FILE = 'client_secrets.json'
# The scope URL for read/write access to a user's gmail and google drive/spreadsheet data
# More scopes can be added, but it is best practice to only ask for access when necessary
SCOPE = 'https://www.googleapis.com/auth/gmail.modify https://spreadsheets.google.com/feeds'

# Create a flow object. This object holds the client_id, client_secret, and
# scope. It assists with OAuth 2.0 steps to get user authorization and
# credentials.
flow = flow_from_clientsecrets(CLIENT_SECRETS_FILE, SCOPE)
#flow = OAuth2WebServerFlow(client_id, client_secret, scope)

def get_credentials():
	"""Gets valid user credentials from storage.

	If nothing has been stored, or if the credentials are invalid, 
	the OAuth2 flow is completed to obtain the new credentials.

	Returns:
		credentials
	"""

	# create a Storage object. This object holds the credentials that your
	# application needs to authorize access to the user's data. The name of the 
	# credentials file is provided. 
	storage = Storage('credentials.dat')

	# The get() function returns teh credentials for the Storage object. If no 
	# credentials are found, None is returned
	credentials = storage.get()

	# If no credentials are found or invalid, new credentials need to be obtained. 
	# The oauth2client.tools.run() function attempts to open an authorization server 
	# page in your default web browser. If the user grants access, the run() function
	# returns new credentials. The new credentials are also stored in teh supplied Storage object. 
	if credentials is None or credentials.invalid:
		credentials = run(flow, storage)

	return credentials

def handle_exception(e):
	print 'An error occured: %s' % e

def list_message_ids(service, user_id, label_ids=[]):
	"""List message_ids of the user's mailbox with label_ids applied. 

	Args:
		service: authorized gmail api service instance.
		user_id: user's email address. value 'me' indicated the authenticated user.
		label_ids: only return messages with these labelIds applied.

	Returns:
		List of message ids that have all required labels applied
	"""

	try:
		# The gmail api's messages().list method returns paginated results, so 
		# we have to execute the request in a paging loop
		response = service.users().messages().list(userId=user_id,
		                                           labelIds=label_ids).execute()
		messages = []
		if 'messages' in response:
			messages.extend(response['messages'])

		while 'nextPageToken' in response:
			page_token = response['nextPageToken']
			response = service.users().messages().list(userId=user_id, 
			                                           labelIds=label_ids,
			                                           pageToken=page_token).execute()
			messages.extend(response['messages'])

		message_ids = [message['id'] for message in messages]
		if len(message_ids) == 0:
			raise ValueError('There are no gmail messages with this label: %s' % label_ids)
		else:
			return message_ids

	except errors.HttpError, error:
		handle_exception(error)


def get_message_data(service, user_id, message_ids):
	"""For a list of message_ids, get the associated messages and parse the 
	   message content to extract the data from an html table embedded in the message. 

	Args:
		service: authorized gmail api service instance.
		user_id: user's email address. 
		message_ids: only get messages with these ids. returned by list_message_ids()

	Returns:
		List of tuples with the table data. Each tuple is of equal length and each 
		item in the tuple represents a column from the table. Each list is unique.
	"""

	message_data = []
	for message_id in message_ids:

		try:
			response = service.users().messages().get(userId=user_id,
			                                         id=message_id).execute()
			if 'parts' not in response['payload']:
				content = response['payload']['body']['data']
			else:
				content = response['payload']['parts'][1]['body']['data']
		except KeyError, error:
			handle_exception(error)
			continue
		except errors.HttpError, error:
			handle_exception(error)
			continue
		else:
			content_decoded = base64.urlsafe_b64decode(content.encode('ASCII'))
			content_cleaned = re.sub('\s+', ' ', content_decoded)	
			content_html = BeautifulSoup(content_cleaned, 'html.parser')	

		try: 
			table_rows = content_html.find('table').findAll('tr')[1:-1]
			if len(table_rows) == 0: 
				print 'This message has no table data: %s' % message_id
				continue
		except AttributeError, error:
			handle_exception(error)

		# Redundant: len(table_row) != 0 at this point
		#for row in filter(lambda x: len(x) > 0, table_rows):
		for row in table_rows:
			data = row.findAll('td')

			name, date, partner, number, customer, description, amount, ship_date, qty, \
			ship_via, warehouse = [td.text.replace(u'\xa0', u' ') for td in data]

			message_data.append([name, date, partner, number, customer, description, 
			                    amount, ship_date, qty, ship_via, warehouse])

	parsed_data = list(set(map(tuple, message_data)))
	if len(parsed_data) == 0:
		raise ValueError('No data for the following messages: %s' % message_ids)
	else:
		return parsed_data

def update_google_sheet(service_gspread, worksheet, data):
	"""Updates the selected spreadsheet with data appended in the first free row. 

	Args:
		service_gspread: service instance for gspread library. A wrapper for the sheets api.
		worksheet: the name of an existing google sheet in the user's google drive
		data: List of lists representing table data. returned by get_message_data()

	Returns:
		Resizes and updates the selected spreadsheet with data. 
	"""

	# Open the target google sheet/worksheet
	try: 
		sheet = service_gspread.open(worksheet).sheet1
	except SpreadsheetNotFound, error:
		raise ValueError('The google sheet does not exist or was referenced incorrectly')

	################################ SET RANGE #############################################
	# Set the google sheet cell range where we will insert the new values by row.
	# The first column is always 'A', the other parameters for the range are variable
	# and based on the number of rows that we need to insert. 
	# Cell_range looks like this: 'A9:K35'
	start_letter = 'A'
	end_letter = string.uppercase[len(data[0]) - 1]
	start_row = len(sheet.col_values(1)) + 1
	end_row = start_row + len(data) -1
	cell_range = '%s%d:%s%d' % (start_letter, start_row, end_letter, end_row)
	########################################################################################

	# Resize the target sheet to add the number of new rows matching the amount of new data
	sheet.resize(rows=end_row)
	# sheet.range() returns a list of gspread Cell objects for the specified range
	cell_list = sheet.range(cell_range)

	# Loop over the data and add each value to the cell_list by index (idx)
	idx = 0
	for row, rowlist in enumerate(data):
		for column, value in enumerate(rowlist):
			cell_list[idx].value = value
			idx += 1
			if idx >= len(cell_list):
				break

	# Append the new cells to the google sheet
	sheet.update_cells(cell_list)

def update_message_labels(service, user_id, message_ids, label_ids=[], new_label_ids=[]):

	for message_id in message_ids:
		message = service.users().messages().modify(userId=user_id,
		                                            id=message_id,
		                                            body={
		                                            	'removeLabelIds': label_ids,
		                                            	'addLabelIds': new_label_ids
		                                            }).execute()

def main():

	credentials = get_credentials()
	http = httplib2.Http()
	http = credentials.authorize(http)

	service = build('gmail', 'v1', http=http)
	service_gsheet = gspread.authorize(credentials)

	ids_list = list_message_ids(service, 'me', message_label)

	data = get_message_data(service, 'me', ids_list)

	update_google_sheet(service_gsheet, select_google_sheet, data)

	update_message_labels(service, 'me', ids_list, message_label, new_message_label)




if __name__ == '__main__':
	main()









