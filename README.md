[![DOI](https://zenodo.org/badge/889865941.svg)](https://doi.org/10.5281/zenodo.14507226)
# Semantic Data Abstraction (SDA)

Welcome to the **Semantic Data Abstraction (SDA)** Python library! This guide will walk you through the steps to get started with the library, including installation, usage, and key components. The SDA library provides tools to integrate and transform data sources into structured target schemas using graph-based transformations, semantic annotations, and rule-based operations.

---

## Table of Contents
- [Semantic Data Abstraction (SDA)](#semantic-data-abstraction-sda)
  - [Table of Contents](#table-of-contents)
  - [Introduction](#introduction)
  - [Features](#features)
  - [Installation](#installation)
  - [Quickstart](#quickstart)
    - [1. Import the Library](#1-import-the-library)
    - [2. Load Source Data and Schema](#2-load-source-data-and-schema)
    - [3. Create an Integration Instance](#3-create-an-integration-instance)
    - [4. Transform the Data](#4-transform-the-data)
  - [Core Components](#core-components)
  - [Contributing](#contributing)
  - [License](#license)

---

## Introduction

The **Semantic Data Abstraction (SDA)** library enables the integration of complex data sources into target data structures using graph-based transformations. It automates processes such as:
- Removing irrelevant data fields
- Flattening hierarchical structures
- Constructing target hierarchies

This library is designed for flexible, efficient, and scalable transformations by leveraging popular Python libraries such as:
- **ReGraph**: For graph manipulation and analysis.
- **PySpark**: For processing large-scale data structures.
- **Pandas**: For data manipulation and integration.

---

## Features

- **Graph-Based Representation**: Interpret data schemas as graphs for flexible and consistent transformation.
- **Semantic Annotations**: Annotate source and target data fields for meaningful integration.
- **Customizable Transformations**: Apply user-defined transformation rules to match data to the target schema.
- **Code Generation**: Generate Python code (e.g., using Pandas or PySpark) to execute transformations.
- **Scalability**: Support for large data sets using PySpark.

---

## Installation

To install the SDA library, you can use `pip`:

```bash
pip install git+https://github.com/JanChristianRedlich/Semantic-Data-Abstraction.git
```

Alternatively, clone the repository and install it locally:

```bash
git clone https://github.com/JanChristianRedlich/Semantic-Data-Abstraction.git
cd sda-library
pip install -e .
```

Ensure that you have Python 3.8+ installed.

---

## Quickstart

Here is a quick example of how to get started with the SDA library:

### 1. Import the Library
```python
from sda_library import SDAIntegration
```

### 2. Load Source Data and Schema
Prepare your source data and schema, including any semantic annotations.

```python
# Example source data (Pandas DataFrame)
source_data = {
    "name": "Alice",
    "street": "Main St",
    "city": "Atlantis"
}

# Example semantic annotations
source_annotation = {
    "name": "personName",
    "address": "streetName",
    "address": "cityName"
}

target_annotation = {
    "person": {
        "name": "personName",
        "address": {
            "street": "streetName",
            "city": "cityName"
        }
    }
}
```

### 3. Create an Integration Instance
Initialize the SDAIntegration class.

```python
sda = SDAIntegration()
sda.readSourceDF(source_data)
sda.addSourceAnnotation(source_annotation)
sda.addTargetAnnotation(target_annotation)
```

### 4. Transform the Data
Perform the transformation of the graphsystem based on the transformation rules to transform the source datashema to the annotated target data shema. 
```python
sda.transform()
```

When the transformation is done you have two options for transforming the data. 
1. Export the code to a py file to work with the code e.g. in real time scenarios or ETL pipelines

```python
f = open("transform.py", "w")
f.write(sda.getPandasCode())
f.close()
```
2. Directly execute the transformation and export the transformed file. 
```python 
transformed_data = sda.runPandasCode("target.json")
print(transformed_data)
```

---

## Core Components

The SDA library consists of the following core components:

1. **SDAIntegration**: The main entry point for performing data integration and transformation. It manages the entire process, from graph interpretation to transformation and code generation.

2. **SDAHirarchicalGraph**: Represents the graph system, including source and target graphs and annotations.

3. **SDATransformation**: Implements the transformation rules and manages the transformation process.

4. **SDATransformOperation**: Represents individual operations, such as removing or linking nodes.

---

## Contributing

We welcome contributions to improve the SDA library! To contribute, please:
1. Fork the repository.
2. Create a new branch for your feature or bugfix.
3. Submit a pull request with a detailed description of your changes.

For guidelines, see the `CONTRIBUTING.md` file.

---

## License

This project is licensed under the MIT License. See the `LICENSE` file for details.

---

Start integrating your data today with the **Semantic Data Abstraction (SDA)** library! For further assistance, feel free to open an issue or submit a pull request.