# packages supporting in the backend
from flask import Flask
from flask import jsonify
from flask import request
from flask_cors import CORS
import networkx as nx
from sklearn.neighbors import KDTree
import pymongo

# application startup
myclient = pymongo.MongoClient('mongodb://localhost:27017/')
mydb = myclient['bdad_app']
streets_collection = mydb['streets']
# directed graph setup
G_crime,G_short = nx.DiGraph(),nx.DiGraph()
G_short.add_nodes_from([x['sid'] for x in streets_collection.find()])
G_crime.add_nodes_from([x['sid'] for x in streets_collection.find()])
cityPoints = []
citySid = []
for x in streets_collection.find():
    citySid.extend([x['sid'] for _ in x['polygons']])
    cityPoints.extend([[p['lat'],p['lng']] for p in x['polygons']])
# create a KD Tree based on city streets
tree = KDTree(cityPoints, leaf_size=2)
for st in streets_collection.find():
    source = st['sid']
    G_crime.add_weighted_edges_from([(source, dst, st['crimeIndex']) for dst in st['neighbors']])
    G_short.add_weighted_edges_from([(source, dst, st['streetLen']) for dst in st['neighbors']])

app = Flask(__name__)
# prevent cross site request forgery
cors = CORS(app, resources={r"/*": {"origins": ["http://localhost:8080",
                                                "http://127.0.0.1:8080"]}})
# backend api address
@app.route("/index",methods=['GET'])
def backend_api():
    global G_crime,G_short,streets_collection,tree,citySid
    startLat = request.args.get('startLat')
    startLng = request.args.get('startLng')
    endLat = request.args.get('endLat')
    endLng = request.args.get('endLng')
    if startLat and startLng and endLat and endLng:
        startPoint,endPoint = [[float(startLat),float(startLng)]],[[float(endLat),float(endLng)]]
        _ ,startStreetPolygonIdx = tree.query(startPoint,k=1)

        start_sid = citySid[startStreetPolygonIdx[0][0]]

        _ ,endStreetPlygonIdx = tree.query(endPoint,k=1)

        print(startStreetPolygonIdx, endStreetPlygonIdx)
        end_sid = citySid[endStreetPlygonIdx[0][0]]
        safe_path_cost, safe_path_sid_lst = nx.single_source_dijkstra(G_crime,start_sid,end_sid)
        short_path_cost, short_path_sid_lst = nx.single_source_dijkstra(G_short,start_sid,end_sid)
        safest = [e for e in streets_collection.find({"sid":{"$in":safe_path_sid_lst}})]
        shortest = [e for e in streets_collection.find({"sid":{"$in":short_path_sid_lst}})]
        for x in safest:
            del x['_id']
            del x['hasDamagedSteetlight']
            del x['hasDrinking']
            del x['hasHomeless']
        for x in shortest:
            del x['_id']
            del x['hasDamagedSteetlight']
            del x['hasDrinking']
            del x['hasHomeless']
        return jsonify({
            "safest": safest,
            "shortest":shortest
        })
    return "Bad Request",400

if __name__ == "__main__":
    app.run()
