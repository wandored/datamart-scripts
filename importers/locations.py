from db_utils.r365_utils import R365Client


client = R365Client()

locations = list(client.get_all("/public/v1/core/locations"))

print(len(locations))
