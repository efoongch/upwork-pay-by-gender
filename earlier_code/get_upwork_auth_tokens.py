import upwork
from pprint import pprint

public_key = raw_input('Please enter public key: > ')
secret_key = raw_input('Please enter secret key: > ')

#Instantiating a client without an auth token
client = upwork.Client(public_key, secret_key)

print "Please to this URL (authorize the app if necessary):"
print client.auth.get_authorize_url()
print "After that you should be redirected back to your app URL with " + \
      "additional ?oauth_verifier= parameter"

verifier = raw_input('Enter oauth_verifier: ')

oauth_access_token, oauth_access_token_secret = \
    client.auth.get_access_token(verifier)

print "This is oauth_access_token"
print oauth_access_token
print "This is oauth_access_token_secret"
print oauth_access_token_secret

# Instantiating a new client, now with a token.
# Not strictly necessary here (could just set `client.oauth_access_token`
# and `client.oauth_access_token_secret`), but typical for web apps,
# which wouldn't probably keep client instances between requests
client = upwork.Client(public_key, secret_key,
                      oauth_access_token=oauth_access_token,
                      oauth_access_token_secret=oauth_access_token_secret)
