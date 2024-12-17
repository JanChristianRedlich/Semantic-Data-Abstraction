import json
import pandas as pd
from .sdahierarchicalgraph import SDAHierarchicalGraph
from .sdatransformation import SDATransformation
from regraph import plot_graph, plot_instance, plot_rule


class SDAIntegration:
    def __init__(self):
        self.hirarchicalGraph = SDAHierarchicalGraph()
        self.transformation = SDATransformation()

    def loadSource(self, jsonSourcePath, sourceAnnotationJsonPath):
        # Construct Hirarchical Graph
        self.hirarchicalGraph.readSourceJSON(jsonSourcePath)
        self.hirarchicalGraph.loadSourceAnnotationsJSON(sourceAnnotationJsonPath)
        self.hirarchicalGraph.createHirarchy()

        plot_graph(self.hirarchicalGraph.hirarchie.get_graph("G"))
        plot_graph(self.hirarchicalGraph.hirarchie.get_graph("S"))
        plot_graph(self.hirarchicalGraph.hirarchie.get_graph("M"))
    
    def doTransformation(self):
        # Remove Irrelevant Nodes
        self.transformation.removeIrrelevantNodes(self.hirarchicalGraph.hirarchie)

        plot_graph(self.hirarchicalGraph.hirarchie.get_graph("G"))
        plot_graph(self.hirarchicalGraph.hirarchie.get_graph("S"))
        plot_graph(self.hirarchicalGraph.hirarchie.get_graph("M"))

        # Flatten the Hirarchy
        self.hirarchicalGraph.add_hom_S_M()
        self.transformation.removeHirarchies(self.hirarchicalGraph.hirarchie)

        plot_graph(self.hirarchicalGraph.hirarchie.get_graph("G"))
        plot_graph(self.hirarchicalGraph.hirarchie.get_graph("S"))
        plot_graph(self.hirarchicalGraph.hirarchie.get_graph("M"))

        #Construct Target Hirarchy
        self.transformation.constructTargetHirarchies(self.hirarchicalGraph.hirarchie, self.hirarchicalGraph.targetSemantic)

        plot_graph(self.hirarchicalGraph.hirarchie.get_graph("G"))
        plot_graph(self.hirarchicalGraph.hirarchie.get_graph("S"))
        plot_graph(self.hirarchicalGraph.hirarchie.get_graph("M"))
        
        # Export FIELD IDs for SELECT
        self.transformation.add_flattening_operations(self.hirarchicalGraph.hirarchie)
        
    def loadTargetAnnotation(self, targetAnnotationJsonPath):
        self.hirarchicalGraph.loadTargetAnnotationJSON(targetAnnotationJsonPath)
    
    
    def transform(self):
        columns_to_select = []
        # Filter DataFrame for Fields to flatten DF
        for operation in self.transformation.normalFormOperations:
            columns_to_select.append(operation.field.replace("root.", ""))
        # Get Pandas DF from pyspark DF
        flatten_df = self.hirarchicalGraph.source_df.select(*columns_to_select)
        flatten_df = flatten_df.toPandas()
        # Rename Columns 
        def create_nested_dict(row, columns_to_nest):
            nested_dict = {}
            for col in columns_to_nest:
                nested_dict[col] = row[col]
            return nested_dict
    
        def create_nested_list(row, columns_to_nest):
            nested_df = {}
            for col in columns_to_nest:
                nested_df[col] = row[col]
            return pd.DataFrame(nested_df).to_dict(orient="records")
        
        for operation in self.transformation.transformOperations:
            if operation.action == "renameNode":
                flatten_df = flatten_df.rename(columns={operation.field.split(".")[-1]: operation.rename})
            elif operation.action == "addHirarchy":
                flatten_df[operation.field] = flatten_df.apply(create_nested_dict, axis=1, columns_to_nest=operation.connect)
                flatten_df = flatten_df.drop(columns=operation.connect)
            elif operation.action == "nestList": 
                flatten_df[operation.field] = flatten_df.apply(create_nested_list, axis=1, columns_to_nest=operation.get_flatten_connect())
                flatten_df = flatten_df.drop(columns=operation.get_flatten_connect())

        return flatten_df


    def getPandasCode(self):
        pass
    
    