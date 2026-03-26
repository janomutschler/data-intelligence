## Custom PySpark Implementation (Reference Branch)

This branch contains an earlier version of the project where the data pipelines were implemented using custom PySpark logic instead of Databricks-native features such as:

- Declarative Pipelines  
- Auto Loader  

The purpose of this implementation was to gain a deeper understanding of how data ingestion and transformation pipelines work under the hood, including:

- Manual schema handling  
- Incremental processing logic  
- Data normalization and transformations  
- Pipeline orchestration patterns  

While this approach required significantly more development effort, it provided valuable insights into the underlying mechanics of modern data pipelines.

The current `main` (`dev`) branch reflects a production-oriented approach, leveraging Databricks-native functionality to improve:

- Maintainability  
- Scalability  
- Reliability  
- Observability  

This branch demonstrates that I am not only capable to use platform features, but also understand and implement the core logic behind them when needed.
