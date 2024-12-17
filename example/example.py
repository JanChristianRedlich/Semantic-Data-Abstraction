from sda.sdaintegration import SDAIntegration
from regraph import plot_graph, plot_instance, plot_rule

# Initialize the SDAIntegration object
sda = SDAIntegration()

# Load the source data and its annotations
# @param source_path: Path to the source data file
# @param source_annotation_path: Path to the source annotation file
sda.loadSource("./example/testfiles/source.json", "./example/testfiles/source_annotation.json")

# Load the target annotation file
# @param target_annotation_path: Path to the target annotation file
sda.loadTargetAnnotation("./example/testfiles/target_annotation.json")

# Perform the transformation logic
sda.doTransformation()

# Transform the data and save it to a JSON file
# @param output_path: Path to save the transformed data as a JSON file
# @param orient: Orientation of the JSON output (e.g., "records")
sda.transform().to_json("./example/testfiles/target.json", orient="records")

# Uncomment the following section to display the operations for transformation
# Show the operations to get the normal form
# print("---- Operations to get Normal Form")
# for op in sda.transformation.normalFormOperations:
#     print(op)

# Show the operations used to transform the data
# print("---- Operations to Transform Data")
# for op in sda.transformation.transformOperations:
#     print(op)

# Plot the executed transformation rules
# @param rule: A rule executed during the transformation process
for rule in sda.transformation.excecutedRules:
    plot_rule(rule)
