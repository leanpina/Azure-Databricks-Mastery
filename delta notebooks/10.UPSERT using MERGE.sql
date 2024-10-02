-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## UPSERT com MERGE no Delta Lake
-- MAGIC
-- MAGIC O conceito de "UPSERT" (UPDATE + INSERT) é uma funcionalidade crucial em sistemas de gerenciamento de dados, permitindo a atualização eficiente de dados existentes e a inserção de novos dados em uma única operação. No contexto do Delta Lake, o comando `MERGE` fornece uma maneira poderosa e flexível de implementar a lógica UPSERT. 
-- MAGIC
-- MAGIC Vamos analisar o funcionamento do UPSERT com `MERGE` com base nas informações fornecidas:
-- MAGIC
-- MAGIC **Entendendo o UPSERT**
-- MAGIC
-- MAGIC * **Objetivo:** O UPSERT visa simplificar o processo de atualização de dados em um sistema, realizando as seguintes ações em uma única operação:
-- MAGIC     * **Atualizar registros existentes:** Se um registro na fonte de dados corresponder a um registro na tabela de destino com base em uma coluna de junção, os valores da fonte serão usados para atualizar o registro correspondente na tabela de destino.
-- MAGIC     * **Inserir novos registros:** Se um registro na fonte de dados não tiver correspondência na tabela de destino (com base na coluna de junção), ele será inserido como um novo registro na tabela de destino.
-- MAGIC
-- MAGIC **O Comando `MERGE`**
-- MAGIC
-- MAGIC * **Sintaxe:** O comando `MERGE` no Delta Lake segue uma estrutura específica para realizar operações UPSERT:
-- MAGIC
-- MAGIC ```sql
-- MAGIC MERGE INTO <tabela_destino> AS destino
-- MAGIC USING <tabela_fonte> AS fonte
-- MAGIC ON <condição_junção>
-- MAGIC WHEN MATCHED THEN
-- MAGIC   <ações_atualização>
-- MAGIC WHEN NOT MATCHED THEN
-- MAGIC   <ações_inserção>;
-- MAGIC ```
-- MAGIC
-- MAGIC * **Elementos-chave:**
-- MAGIC
-- MAGIC     * **`MERGE INTO <tabela_destino> AS destino`:** Especifica a tabela Delta de destino onde as operações de atualização ou inserção serão realizadas.
-- MAGIC
-- MAGIC     * **`USING <tabela_fonte> AS fonte`:** Indica a tabela ou fonte de dados que contém os dados a serem mesclados na tabela de destino.
-- MAGIC
-- MAGIC     * **`ON <condição_junção>`:** Define a condição de junção usada para combinar registros da fonte e da tabela de destino, normalmente com base em uma coluna comum.
-- MAGIC
-- MAGIC     * **`WHEN MATCHED THEN <ações_atualização>`:** Especifica as ações a serem executadas quando um registro da fonte corresponder a um registro da tabela de destino com base na condição de junção. Normalmente, isso envolve a atualização de colunas na tabela de destino com valores da fonte.
-- MAGIC
-- MAGIC     * **`WHEN NOT MATCHED THEN <ações_inserção>`:** Determina as ações a serem realizadas quando um registro da fonte não tiver correspondência na tabela de destino. Geralmente, isso envolve a inserção de um novo registro na tabela de destino com valores da fonte.
-- MAGIC
-- MAGIC **Exemplo Prático:**
-- MAGIC
-- MAGIC As fontes fornecem um exemplo prático de como o comando `MERGE` é usado para implementar a lógica UPSERT no Delta Lake. O cenário envolve duas tabelas:
-- MAGIC
-- MAGIC * **`source_table`:** Contém os dados que serão mesclados na tabela de destino.
-- MAGIC * **`destination_table`:** A tabela Delta de destino que será atualizada ou receberá novas inserções.
-- MAGIC
-- MAGIC O exemplo demonstra como atualizar registros existentes na `destination_table` com base em valores correspondentes na `source_table` e como inserir novos registros da `source_table` na `destination_table` quando não houver correspondência.
-- MAGIC
-- MAGIC **Benefícios do UPSERT com `MERGE` no Delta Lake:**
-- MAGIC
-- MAGIC * **Eficiência:** Realiza atualizações e inserções em uma única operação, otimizando o processo.
-- MAGIC * **Atomicidade:** Garante que a operação seja atômica, ou seja, todas as atualizações e inserções são aplicadas como uma única unidade de trabalho, evitando inconsistências.
-- MAGIC * **Manutenção de Histórico:** Como todas as operações no Delta Lake são registradas no log de transações, as operações UPSERT com `MERGE` também são versionadas, permitindo auditoria e reversão.
-- MAGIC
-- MAGIC **Cenários de Uso:**
-- MAGIC
-- MAGIC O UPSERT com `MERGE` é particularmente útil em cenários como:
-- MAGIC
-- MAGIC * **Cargas incrementais:** Manter uma tabela Delta atualizada com dados provenientes de um sistema transacional, atualizando registros existentes e inserindo novos.
-- MAGIC * **Sincronização de dados:** Manter duas ou mais tabelas Delta sincronizadas, garantindo que as alterações em uma tabela sejam refletidas nas outras.
-- MAGIC
-- MAGIC Em resumo, o comando `MERGE` no Delta Lake fornece uma maneira robusta e eficiente de implementar a lógica UPSERT, simplificando o processo de atualização de dados e garantindo a integridade dos dados. Compreender seus elementos-chave, benefícios e casos de uso é essencial para utilizar totalmente os recursos do Delta Lake para o gerenciamento de dados.
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Creating a source Table

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Como não foi especificado o catálogo, essa tabela será criada em "default"

-- COMMAND ----------

CREATE TABLE Source_Table
(
    Education_Level VARCHAR(50),
    Line_Number INT,
    Employed INT,
    Unemployed INT,
    Industry VARCHAR(50),
    Gender VARCHAR(10),
    Date_Inserted DATE,
    dense_rank INT
)

-- COMMAND ----------

INSERT INTO Source_Table
VALUES
    ('Bachelor', 100, 4500, 500, 'Networking', 'Male', '2023-07-12', 1),
    ('Master', 101, 6500, 1500, 'Networking', 'Female', '2023-07-12', 2),
    ('Master', 103, 5500, 500, 'Networking', 'Female', '2023-07-12', 3);

-- COMMAND ----------

SELECT * FROM source_table


-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Creating destination table

-- COMMAND ----------

CREATE TABLE `delta`.Dest_Table
(
    Education_Level VARCHAR(50),
    Line_Number INT,
    Employed INT,
    Unemployed INT,
    Industry VARCHAR(50),
    Gender VARCHAR(10),
    Date_Inserted DATE,
    dense_rank INT
)

-- COMMAND ----------

INSERT INTO delta.Dest_Table
VALUES
    ('Bachelor', 100, 1500, 1500, 'Networking', 'Male', '2023-07-12', 1),
    ('Master', 101, 2500, 2000, 'Networking', 'Female', '2023-07-12', 2);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Applying UPSERT using MERGE

-- COMMAND ----------

MERGE INTO `delta`.Dest_Table AS Dest
USING Source_Table as Source
    on Dest.Line_Number = Source.Line_Number
  WHEN MATCHED
    THEN UPDATE SET
  Dest.Education_Level = Source.Education_Level,
  Dest.Line_Number = Source.Line_Number,
  Dest.Employed = Source.Employed,
  Dest.Unemployed = Source.Unemployed,
  Dest.Industry = Source.Industry,
  Dest.Gender = Source.Gender,
  Dest.Date_Inserted = Source.Date_Inserted,
  Dest.dense_rank = Source.dense_rank

  WHEN NOT MATCHED
  THEN INSERT
    (Education_Level, Line_Number, Employed, Unemployed, Industry, Gender, Date_Inserted, dense_rank)
    VALUES(Source.Education_Level, Source.Line_Number, Source.Employed, Source.Unemployed, Source.Industry, Source.Gender, Source.Date_Inserted, Source.dense_rank)

-- COMMAND ----------

SELECT * FROM `delta`.Dest_Table 

-- COMMAND ----------


