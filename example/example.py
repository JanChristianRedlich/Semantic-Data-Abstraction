from sda.sdaintegration import SDAIntegration
from regraph import plot_graph, plot_instance, plot_rule

sda = SDAIntegration()

sda.loadSource("./example/testfiles/source.json", "./example/testfiles/source_annotation.json") 
sda.loadTargetAnnotation("./example/testfiles/target_annotation.json")
sda.doTransformation()
sda.transform().to_json("./example/testfiles/target.json", orient="records")

# Show Operations to transform the data
# print("---- Operations to get Normal Form")
# for op in sda.transformation.normalFormOperations:
#     print(op)
# print("---- Operations to Transform Data")
# for op in sda.transformation.transformOperations:
#     print(op)

for rule in sda.transformation.excecutedRules:
    plot_rule(rule)