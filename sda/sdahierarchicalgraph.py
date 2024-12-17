from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, ArrayType
import matplotlib.pyplot as plt
import json

from regraph import NXGraph, Rule, NXHierarchy
from regraph import plot_graph, plot_instance, plot_rule

class SDAHierarchicalGraph:
    def __init__(self):
        self.G = None
        self.S = None
        self.M = self.get_meta_graph()
        self.hirarchie = None
        self.targetSemantic = None
        self.source_df = None

        self.spark = SparkSession \
            .builder \
            .config("spark.driver.bindAddress", "127.0.0.1") \
            .config("spark.driver.host", "localhost") \
            .config("spark.driver.port", "4040") \
            .appName("sda") \
            .getOrCreate()

    
    def addAnnotations(self, annotation):
        self.S = self.get_annotation_graph(annotation)
    
    def loadSourceAnnotationsJSON(self, json_file_path):
        annotation_file = open(json_file_path)
        self.addAnnotations(json.load(annotation_file))

    def loadTargetAnnotationJSON(self, json_file_path):
        annotation_file = open(json_file_path)
        self.targetSemantic = json.load(annotation_file)
    
    def createHirarchy(self):
        self.hirarchie = NXHierarchy()
        self.hirarchie.add_graph("G", self.remove_attributes(self.G), {"name": "Source Schema"})
        # Add Source Semantic
        self.hirarchie.add_graph("S", self.remove_attributes(self.S), {"name": "Source Annotations"})

        self.hirarchie.add_typing(
            "G", "S",
            self.get_schema_annotation_mapping(self.G,self.S)
        )
        # Add Meta Model
        self.hirarchie.add_graph("M", self.remove_attributes(self.M), {"name": "Meta Model"})
        self.hirarchie.add_typing(
            "G", "M",
            self.get_meta_mapping_G(self.G)
        )
    
    def readSourceDF(self, df):
        self.G = self.get_schema_graph(df.schema)
    
    def readSourceJSON(self, json_file_path):
        self.source_df = self.spark.read.option("multiline", "true").json(json_file_path)
        self.readSourceDF(self.source_df)
    
    ##############

    def get_schema_graph(self, schema, parent="root", G=None, S=None):
        if G is None:
            G = NXGraph()
            G.add_node('root', {"type":"root", "key_name":"root"})

        for field in schema.fields:
            f_name = parent + "." + field.name
            if isinstance(field.dataType, StructType):
                G.add_node(f_name, {"key_name":field.name, "type":'struct', "dataType":field.dataType.simpleString(), "nullable":field.nullable, "metadata":field.metadata})
                G.add_edge(parent, f_name)

                self.get_schema_graph(field.dataType, f_name, G)
            elif isinstance(field.dataType, ArrayType) and isinstance(field.dataType.elementType, StructType):
                G.add_node(f_name, {"key_name":field.name, "type":'array', "dataType":field.dataType.simpleString(), "nullable":field.nullable, "metadata":field.metadata})
                G.add_edge(parent, f_name)
                self.get_schema_graph(field.dataType.elementType, f_name, G)
            else:
                G.add_node(f_name, {"key_name":field.name, "type":'field', "dataType":field.dataType.simpleString(), "nullable":field.nullable, "metadata":field.metadata})
                G.add_edge(parent, f_name)

        return G

    def get_meta_graph(self):
        M = NXGraph()
        M.add_node('ROOT', {"type":"root", "key_name":"root"})

        M.add_node("STRUCT", {})
        M.add_edge("STRUCT", "STRUCT")
        M.add_edge("ROOT", "STRUCT")


        M.add_node("ARRAY", {})
        M.add_edge("STRUCT", "ARRAY")
        M.add_edge("ROOT", "ARRAY")
        M.add_edge("ARRAY", "STRUCT")

        M.add_node("FIELD", {})
        M.add_edge("STRUCT", "FIELD")
        M.add_edge("ROOT", "FIELD")
        M.add_edge("ARRAY", "FIELD")
        
        return M

    def get_meta_mapping_G(self, G):
        mapping = {}

        for node in G.nodes(data=True):
            node_id, attrs = node

            if node_id == "root":
                mapping[node_id] = "ROOT"
            elif list(attrs['type'])[0] == "struct":
                mapping[node_id] = "STRUCT"
            elif list(attrs['type'])[0] == "field":
                mapping[node_id] = "FIELD"
            elif list(attrs['type'])[0] == "array":
                mapping[node_id] = "ARRAY"
            else:
                mapping[node_id] = "FIELD"

        return mapping

    def get_meta_mapping_S(self, S):
        mapping = {}

        for node in S.nodes(data=True):
            node_id, attrs = node

            if node_id == "root":
                mapping[node_id] = "ROOT"
            elif "DICT" in node_id:
                mapping[node_id] = "STRUCT"
            elif "DELETE" in node_id:
                pass
            elif "LIST" in node_id:#list(attrs['type'])[0] == "array":
                mapping[node_id] = "ARRAY"
            else:
                mapping[node_id] = "FIELD"

        return mapping

    def get_meta_mapping_M(self, G):
        mapping = {}

        for node in G.nodes(data=True):
            node_id, attrs = node

            if node_id == "root":
                mapping[node_id] = "ROOT"
            elif "DICT" in node_id:
                mapping[node_id] = "STRUCT"
            elif "DELETE" in node_id:
                pass
            # SODO: ADD List and real field Handeling
            # elif list(attrs['type'])[0] == "field":
            #     mapping[node_id] = "FIELD"
            # elif list(attrs['type'])[0] == "array":
            #     mapping[node_id] = "ARRAY"
            else:
                mapping[node_id] = "FIELD"

        return mapping

    def get_meta_mapping_S_G(self, G):
        mapping = {}

        for node in G.get_graph("G").nodes():
            mapping[G.node_type("G", node)['M']] = str(node)

        return mapping

    def get_annotation_graph(self, annotation, parent_id="root", parent="root", S=None):
        if S is None:
            S = NXGraph()
            S.add_node('root', {"type":"root", "key_name":"root"})
            S.add_node('DELETE')
            S.add_edge("root", "DELETE")
            S.add_edge("DELETE", "DELETE")

        for key, value in annotation.items():
            f_name = parent_id + "." + key
            if isinstance(value, dict):
                name = parent + ".DICT"
                S.add_node(name, {"key_name":key, "id":f_name, "type":'struct'})
                S.add_edge(parent, name)
                S.add_edge(name, "DELETE")

                self.get_annotation_graph(value, f_name, name, S)
            elif isinstance(value, list):
                name = parent + ".LIST"
                S.add_node(name, {"key_name":key, "id":f_name, "type":'array'})
                S.add_edge(parent, name)
                S.add_edge(name, "DELETE")

                self.get_annotation_graph(value[0], f_name, name, S)
            else:
                S.add_node(value, {"key_name":key, "id":f_name, "type":'field'})
                S.add_edge(parent, value)

        return S

    def get_schema_annotation_mapping(self, G,S):
        mapping = {}
        annotations = {}

        for node in S.nodes(data=True):
            node_type, attrs = node
            if "id" in attrs.keys():
                for x in attrs['id'].fset:
                    node_id = x
                annotations[node_id] = node_type
            

        for node in G.nodes(data=True):
            node_id, attrs = node
            if node_id == "root":
                mapping[node_id] = "root"
            elif node_id in annotations.keys():
                mapping[node_id] = annotations[node_id]
            else:
                mapping[node_id] = "DELETE"
        print(mapping)
        return mapping

    def remove_attributes(self, G):
        G = NXGraph.copy(G)
        for node in G.nodes(data=True):
            node_id, attrs = node

            G.update_node_attrs(node_id, {})

        return G
    
    def add_hom_S_M(self):
            # Add Missing Homomorphism
        self.hirarchie.add_typing(
            "S", "M",
            self.get_meta_mapping_S(self.S)
        )