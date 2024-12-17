import json
import pandas as pd
from .sdahierarchicalgraph import SDAHierarchicalGraph
from .sdatransformation import SDATransformation
from regraph import plot_graph, plot_instance, plot_rule


class SDAIntegration:
    """
    A class to handle the integration of source data and its transformation into a hierarchical graph,
    followed by further processing and manipulation.
    """

    def __init__(self):
        """
        Initializes the SDAIntegration class by creating instances of SDAHierarchicalGraph
        and SDATransformation.
        """
        self.hirarchicalGraph = SDAHierarchicalGraph()
        self.transformation = SDATransformation()

    def loadSource(self, jsonSourcePath, sourceAnnotationJsonPath):
        """
        Loads the source JSON data and its annotations, constructs a hierarchical graph,
        and visualizes the different parts of the hierarchy.

        Args:
            jsonSourcePath (str): Path to the JSON file containing the source data.
            sourceAnnotationJsonPath (str): Path to the JSON file containing source annotations.
        """
        # Construct Hierarchical Graph
        self.hirarchicalGraph.readSourceJSON(jsonSourcePath)
        self.hirarchicalGraph.loadSourceAnnotationsJSON(sourceAnnotationJsonPath)
        self.hirarchicalGraph.createHirarchy()

        # Visualize the constructed hierarchy
        plot_graph(self.hirarchicalGraph.hirarchie.get_graph("G"))
        plot_graph(self.hirarchicalGraph.hirarchie.get_graph("S"))
        plot_graph(self.hirarchicalGraph.hirarchie.get_graph("M"))

    def doTransformation(self):
        """
        Executes a series of transformations on the hierarchical graph, including
        removing irrelevant nodes, flattening the hierarchy, constructing target hierarchies,
        and adding flattening operations. Visualizes the hierarchy at each step.
        """
        # Remove Irrelevant Nodes
        self.transformation.removeIrrelevantNodes(self.hirarchicalGraph.hirarchie)
        plot_graph(self.hirarchicalGraph.hirarchie.get_graph("G"))
        plot_graph(self.hirarchicalGraph.hirarchie.get_graph("S"))
        plot_graph(self.hirarchicalGraph.hirarchie.get_graph("M"))

        # Flatten the Hierarchy
        self.hirarchicalGraph.add_hom_S_M()
        self.transformation.removeHirarchies(self.hirarchicalGraph.hirarchie)
        plot_graph(self.hirarchicalGraph.hirarchie.get_graph("G"))
        plot_graph(self.hirarchicalGraph.hirarchie.get_graph("S"))
        plot_graph(self.hirarchicalGraph.hirarchie.get_graph("M"))

        # Construct Target Hierarchy
        self.transformation.constructTargetHirarchies(self.hirarchicalGraph.hirarchie, self.hirarchicalGraph.targetSemantic)
        plot_graph(self.hirarchicalGraph.hirarchie.get_graph("G"))
        plot_graph(self.hirarchicalGraph.hirarchie.get_graph("S"))
        plot_graph(self.hirarchicalGraph.hirarchie.get_graph("M"))

        # Export FIELD IDs for SELECT
        self.transformation.add_flattening_operations(self.hirarchicalGraph.hirarchie)

    def loadTargetAnnotation(self, targetAnnotationJsonPath):
        """
        Loads target annotation data into the hierarchical graph.

        Args:
            targetAnnotationJsonPath (str): Path to the JSON file containing target annotations.
        """
        self.hirarchicalGraph.loadTargetAnnotationJSON(targetAnnotationJsonPath)

    def transform(self):
        """
        Applies transformations to the source data, including renaming nodes, adding hierarchies,
        and nesting lists, based on the specified operations. Returns a flattened pandas DataFrame.

        Returns:
            pandas.DataFrame: Transformed and flattened data.
        """
        columns_to_select = []

        # Gather columns to select for flattening operations
        for operation in self.transformation.normalFormOperations:
            columns_to_select.append(operation.field.replace("root.", ""))

        # Convert source DataFrame to pandas DataFrame
        flatten_df = self.hirarchicalGraph.source_df.select(*columns_to_select)
        flatten_df = flatten_df.toPandas()

        # Helper functions for creating nested structures
        def create_nested_dict(row, columns_to_nest):
            """
            Creates a nested dictionary from specified columns in a row.

            Args:
                row (pandas.Series): The row of the DataFrame.
                columns_to_nest (list): List of column names to nest.

            Returns:
                dict: Nested dictionary.
            """
            nested_dict = {}
            for col in columns_to_nest:
                nested_dict[col] = row[col]
            return nested_dict

        def create_nested_list(row, columns_to_nest):
            """
            Creates a nested list from specified columns in a row.

            Args:
                row (pandas.Series): The row of the DataFrame.
                columns_to_nest (list): List of column names to nest.

            Returns:
                list: Nested list of dictionaries.
            """
            nested_df = {}
            for col in columns_to_nest:
                nested_df[col] = row[col]
            return pd.DataFrame(nested_df).to_dict(orient="records")

        # Apply transformations based on operations
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
        """
        Placeholder function for generating pandas code, if required.
        """
        pass
