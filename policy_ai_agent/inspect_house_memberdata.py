# inspect_house_memberdata.py
import requests
import xml.etree.ElementTree as ET
from io import BytesIO

URL = "https://clerk.house.gov/xml/lists/MemberData.xml"

resp = requests.get(URL, timeout=60)
resp.raise_for_status()

root = ET.parse(BytesIO(resp.content)).getroot()

print("Root tag:", root.tag)
print("Top-level children:", [child.tag for child in list(root)[:10]])

members = root.findall(".//member")
print("Member count:", len(members))

if members:
    m = members[0]
    print("First member child tags:", [child.tag for child in list(m)])

    info = m.find("member-info")
    if info is not None:
        print("member-info tags:", [child.tag for child in list(info)])

    comm_assign = m.find("committee-assignments")
    print("committee-assignments exists:", comm_assign is not None)
    if comm_assign is not None:
        print("committee-assignments child tags:", [child.tag for child in list(comm_assign)[:20]])

        committees = comm_assign.findall("committee")
        print("committee count under first member:", len(committees))
        if committees:
            c = committees[0]
            print("first committee tag attrs:", c.attrib)
            print("first committee child tags:", [child.tag for child in list(c)])
            subs = c.findall("subcommittee")
            print("subcommittee count under first committee:", len(subs))
            if subs:
                s = subs[0]
                print("first subcommittee attrs:", s.attrib)
                print("first subcommittee child tags:", [child.tag for child in list(s)])