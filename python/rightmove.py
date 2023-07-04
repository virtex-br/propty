from rightmove_webscraper import RightmoveData
import json
import requests
import re
def get_properties(postcode, radius=1):
    # add postcode here:
    l = "https://www.rightmove.co.uk/house-prices/{}.html".format(postcode.replace(' ','-').lower())
    html_text = requests.get(l).text
    data = re.search(r"__PRELOADED_STATE__ = ({.*?})<", html_text)
    data = json.loads(data.group(1))
    # print(json.dumps(data, indent=4))
    location_id = data["searchLocation"]["locationId"]
    url = "https://www.rightmove.co.uk/property-for-sale/find.html?locationIdentifier=POSTCODE^{}&radius={}".format(location_id, radius)
    rm = RightmoveData(url)
    return rm