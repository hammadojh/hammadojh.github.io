import requests
import geopy.distance
from datetime import datetime, timedelta
import json
import time
import boto3
import plotly.express as px
import math
import pandas as pd
import io
from slack import WebClient
from slack.errors import SlackApiError

px.set_mapbox_access_token("pk.eyJ1IjoiaGFtbWFkb2poIiwiYSI6ImNsYWs5bHZlYjBoczkzcG9lczF6N3J0ejEifQ.AUL3NVajNNctC5tYGPKFfg")
s3 = boto3.resource('s3')

center=[39.780307, -104.964156]
radius=10000

def distance(p1,p2):
    return geopy.distance.geodesic(p1, p2).m

def in_circle(point,center=center,radius=radius):
    return distance(center,point) < radius

def filter_road_conditions(orignal_features):
    return list(filter(lambda x: in_circle([x["properties"]["primaryLatitude"],x["properties"]["primaryLongitude"]]),orignal_features))

def lambda_handler(event, context):
    
    # call them 
    events = requests.get("https://data.cotrip.org/api/v1/plannedEvents?apiKey=J2N7GKZ-37Q4XSB-HXDQKMJ-6EEQH88").json()
    print("total events %d" % len(events["features"]))

    #clean events
    for e in events["features"]:
        point = []
        if type(e["geometry"]["coordinates"][0]) != list:
            e["geometry"]["coordinates"] = [[e["geometry"]["coordinates"][0],e["geometry"]["coordinates"][1]]]

    #filter events 
    close_events = list(filter(lambda x: in_circle([x["geometry"]["coordinates"][0][1],x["geometry"]["coordinates"][0][0]]),events["features"]))
    print("filtered events %d" % len(close_events))
    
    #filter by today or tomorrow
    tomorrow = datetime.today() + timedelta(days=2)
    #timely_events = list(filter(lambda x: datetime.strptime(x["properties"]["startTime"],'%Y-%m-%dT%H:%M:%SZ') < tomorrow , close_events))
    timely_events = list(filter(lambda x: "2022-12-07" in x["properties"]["startTime"] or "2022-12-08" in x["properties"]["startTime"] , close_events))
    timely_events.sort(key=lambda x: x["properties"]["startTime"])
    
    #draw a map
    radius = 0.001
    points = pd.DataFrame({"lon":[],"lat":[]})
        
    #return them
    for event in timely_events:
        
        print("event")
        
        # if sent skip
        old = []
        filename = "sent_messages.txt" 
        s3_object = s3.Object('pureconnectinfo', filename)
        old = s3_object.get()["Body"].read().decode("utf-8").split("\n")
        
        print(event)
        travellerMessage = event["properties"]["travelerInformationMessage"]
        
        # if (travellerMessage) in old:
        #     continue

        print("new")
        
        #add circle points 
        center = (event["geometry"]["coordinates"][0][1],event["geometry"]["coordinates"][0][0])
        for angle in range(0, 360):
            x = center[0] + radius * math.cos(math.radians(angle))
            y = center[1] + radius * math.sin(math.radians(angle))
            points = pd.concat([points,pd.DataFrame([[y,x]],columns=["lon","lat"])])

        
        # else write 
        new_string = "\n".join(old) + "\n" + travellerMessage
        # s3_object.put(Body= new_string)
            
        # prep for slack
        event_time = datetime.strptime(event["properties"]["startTime"],'%Y-%m-%dT%H:%M:%SZ')
        time_string = datetime.strftime(event_time,'%Y-%m-%dT%H:%M')
        string = "\n*A planned event that will start at %s* \n" % time_string
        string += event["properties"]["travelerInformationMessage"]
        
        # send
        url = "https://hooks.slack.com/services/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"
        payload = {"text":string}
        #requests.post(url,json.dumps(payload))
        #time.sleep(5)
        
    
    # make fig after all events were sent
    print(points)
    if len(points) > 0:
        fig = px.scatter_mapbox(points.iloc[1:,:], lat="lat", lon="lon")
        fig.update_layout(
        geo = dict(
            projection_scale=10 #this is kind of like zoom
        ))
        today = str(datetime.today().strftime("%Y-%m-%d"))
        filename = "fig_%s" % today
        fig.write_image(filename+".png") # save fig
        fig.write_html(filename+".html")
        img = open(filename+".png", 'rb').read()
        client = WebClient("xoxb-4102765264131-4358632729024-7gxqNW2gNcACVjdr2l8RGLJR")
        # client.files_upload(
        #     channels = "C0433DFE0SG",
        #     initial_comment = "Map of previous points",
        #     filename = "Map",
        #     content = img)
    
    
    return {
        'statusCode': 200,
        'body': json.dumps('Success')
    }


lambda_handler({},{})
