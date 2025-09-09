from py4lexis.core.lexis_irods import iRODS
from py4lexis.core.session import LexisSessionOffline



# with open("refresh_token.txt", "r") as text_file:
#     access_token = text_file.read()
from py4lexis.session import LexisSession
lexis_session = LexisSession(offline_access=True)
refresh_token = lexis_session.get_refresh_token()
access_token = lexis_session.get_access_token()
offline_empty_token = lexis_session.get_offline_token()

lexis_session.refresh_token(refresh_token)
offline_token = lexis_session.get_offline_token()
# lexis_session.refresh_token()
with open("refresh_token.txt", "w") as text_file:
     text_file.write(refresh_token)
#
with open("access_token.txt", "w") as text_file:
     text_file.write(access_token)
#
with open("offline_token.txt", "w") as text_file:
     text_file.write(offline_token)

