-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Demonstração de Row Level Security e Column Masking
-- MAGIC
-- MAGIC Este notebook demonstra como implementar e utilizar Row Level Security (RLS) e Column Masking no Databricks Unity Catalog.
-- MAGIC
-- MAGIC ## Documentação Oficial
-- MAGIC - [Row Level Security & Column Masking](https://docs.databricks.com/aws/en/tables/row-and-column-filters)
-- MAGIC
-- MAGIC ## Cenário de Demonstração
-- MAGIC Vamos criar um cenário prático onde:
-- MAGIC 1. Implementamos controle de acesso baseado em departamentos usando Row Level Security
-- MAGIC 2. Protegemos dados sensíveis (CPF e salário) usando Column Masking
-- MAGIC 3. Criamos uma tabela de mapeamento para controlar usuários com acesso especial
-- MAGIC
-- MAGIC ### Regras de Acesso
-- MAGIC - Usuários com acesso especial podem ver dados de seu próprio departamento
-- MAGIC - CPF e salário são mascarados para outros usuários (Somente você pode ver seu próprio CPF e salário)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. Configuração do Schema
-- MAGIC
-- MAGIC Primeiro, vamos configurar o schema no Unity Catalog usando o nome do usuário.
-- MAGIC Esta é uma prática recomendada para garantir isolamento de dados entre diferentes usuários.

-- COMMAND ----------

DECLARE OR REPLACE VARIABLE user_name STRING;
DECLARE OR REPLACE VARIABLE schema_name STRING;
SET VARIABLE user_name = '<your_user_handle>';
SET VARIABLE schema_name = user_name;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. Configurar Catalog e Schema
-- MAGIC
-- MAGIC Configuramos o catalog `dev_hands_on` e o schema específico do usuário para garantir
-- MAGIC que todas as operações subsequentes sejam executadas no contexto correto.

-- COMMAND ----------

USE CATALOG users;
USE SCHEMA IDENTIFIER(schema_name);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3. Criar Tabela de Exemplo
-- MAGIC
-- MAGIC Vamos criar uma tabela Delta com dados de funcionários que será utilizada para
-- MAGIC demonstrar as funcionalidades de RLS e Column Masking.
-- MAGIC
-- MAGIC A tabela contém:
-- MAGIC - Informações básicas (id, nome, departamento)
-- MAGIC - Dados sensíveis (CPF, salário)
-- MAGIC - Data de admissão

-- COMMAND ----------

-- DBTITLE 1,Criar tabela e inserir dados
-- Criando uma tabela Delta com dados de funcionários
CREATE OR REPLACE TABLE funcionarios (
  id INT,
  nome STRING,
  departamento STRING,
  salario DECIMAL(10,2),
  cpf STRING,
  data_admissao DATE
) USING DELTA;

-- Inserindo dados de exemplo
INSERT INTO funcionarios VALUES
  (1, 'João Silva', 'TI', 5000.00, '123.456.789-00', '2023-01-15'),
  (2, 'Maria Santos', 'RH', 4500.00, '987.654.321-00', '2023-02-20'),
  (3, 'Pedro Oliveira', 'TI', 4800.00, '456.789.123-00', '2023-03-10'),
  (4, 'Ana Costa', 'Financeiro', 5500.00, '789.123.456-00', '2023-04-05'),
  (5, 'Carlos Santos', 'Financeiro', 6000.00, '321.654.987-00', '2023-05-01'),
  (6, schema_name, 'Financeiro', 6000.00, '123.456.789-10', '2023-05-01');

-- COMMAND ----------

-- DBTITLE 1,Verificar dados originais
-- Verificando os dados originais antes de aplicar RLS
SELECT * FROM funcionarios;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4. Criar Mapping Table
-- MAGIC
-- MAGIC A tabela de mapeamento (`usuarios_acesso_especial`) é fundamental para o controle de acesso.
-- MAGIC Ela define quais usuários têm permissões especiais e a quais departamentos eles têm acesso.
-- MAGIC
-- MAGIC Esta tabela será utilizada tanto para o RLS quanto para o Column Masking.
-- MAGIC
-- MAGIC ### Alternativas à Mapping Table
-- MAGIC
-- MAGIC Embora nesta demo utilizemos uma tabela de mapeamento para facilitar a demonstração e testes,
-- MAGIC em ambientes de produção é comum utilizar funções nativas do Databricks que integram
-- MAGIC diretamente com o sistema de identidade (Identity Provider) e grupos de usuários.
-- MAGIC
-- MAGIC #### Funções Disponíveis
-- MAGIC - `CURRENT_USER()`: Retorna o email do usuário atual
-- MAGIC - `IS_ACCOUNT_GROUP_MEMBER('grupo')`: Verifica se o usuário pertence a um grupo específico
-- MAGIC - `IS_MEMBER('grupo')`: Verifica se o usuário é membro de um grupo no workspace
-- MAGIC
-- MAGIC #### Exemplos de Implementação
-- MAGIC
-- MAGIC **Row Level Security com Grupos:**
-- MAGIC ```sql
-- MAGIC CREATE FUNCTION dept_access(department STRING)
-- MAGIC RETURN IS_ACCOUNT_GROUP_MEMBER('admin')
-- MAGIC   OR IS_ACCOUNT_GROUP_MEMBER(department);
-- MAGIC ```
-- MAGIC
-- MAGIC **Column Masking com Grupos:**
-- MAGIC ```sql
-- MAGIC CREATE FUNCTION salary_mask(salary DECIMAL)
-- MAGIC RETURN CASE
-- MAGIC   WHEN IS_ACCOUNT_GROUP_MEMBER('finance_admin') THEN salary
-- MAGIC   WHEN IS_ACCOUNT_GROUP_MEMBER('finance_read') THEN ROUND(salary, -3)
-- MAGIC   ELSE 0
-- MAGIC END;
-- MAGIC ```
-- MAGIC
-- MAGIC **Combinando Múltiplas Validações:**
-- MAGIC ```sql
-- MAGIC CREATE FUNCTION sensitive_data_access(department STRING)
-- MAGIC RETURN IS_ACCOUNT_GROUP_MEMBER('admin')
-- MAGIC   OR (IS_ACCOUNT_GROUP_MEMBER('data_scientist') AND department = 'Research')
-- MAGIC   OR (IS_MEMBER('power_user') AND CURRENT_USER() LIKE '%@company.com');
-- MAGIC ```
-- MAGIC
-- MAGIC Estas abordagens são mais seguras e escaláveis, pois:
-- MAGIC - Integram diretamente com o sistema de gestão de identidade
-- MAGIC - Não requerem manutenção de tabelas de mapeamento
-- MAGIC - Atualizam automaticamente quando usuários são adicionados/removidos de grupos
-- MAGIC - São mais difíceis de burlar ou manipular

-- COMMAND ----------

-- DBTITLE 1,Criar mapping table
-- Criando uma tabela para mapear usuários com acesso especial
CREATE OR REPLACE TABLE usuarios_acesso_especial (
  email STRING,
  departamento STRING,
  CPF STRING
) USING DELTA;

-- Inserindo alguns usuários com acesso especial
INSERT INTO usuarios_acesso_especial VALUES
  ('admin@databricks.com', 'TI', '0000'),
  ('rh@databricks.com', 'RH', '0000');

-- COMMAND ----------

-- DBTITLE 1,Select na tabela que controla os acessos
SELECT * FROM usuarios_acesso_especial

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 5. Implementar Row Level Security
-- MAGIC
-- MAGIC O Row Level Security (RLS) é implementado através de uma função UDF que verifica se o usuário
-- MAGIC está na tabela de mapeamento e tem acesso ao departamento específico.
-- MAGIC
-- MAGIC A função `acesso_especial_filter`:
-- MAGIC 1. Verifica se o usuário atual está na tabela de mapeamento
-- MAGIC 2. Confirma se o usuário tem acesso ao departamento específico
-- MAGIC 3. Retorna true apenas se ambas as condições forem atendidas

-- COMMAND ----------

-- DBTITLE 1,Criar função UDF para RLS
-- Criando função UDF que verifica se o usuário está na mapping table
CREATE OR REPLACE FUNCTION acesso_especial_filter (departamento STRING)
RETURN EXISTS(
  SELECT 1 FROM usuarios_acesso_especial u
  WHERE u.email = CURRENT_USER()
  AND u.departamento = departamento
) and departamento in (select departamento from usuarios_acesso_especial f where f.email = CURRENT_USER())

-- COMMAND ----------

-- DBTITLE 1,Select antes de aplicar RLS
-- Verificando os dados originais antes de aplicar RLS
SELECT * FROM funcionarios;

-- COMMAND ----------

-- DBTITLE 1,Aplicar política de RLS
-- Aplicando a política de RLS à tabela
ALTER TABLE funcionarios
SET ROW FILTER acesso_especial_filter ON (departamento);

-- COMMAND ----------

-- DBTITLE 1,Realizando na tabela com RLS
-- Verificando como o usuário atual vê os dados
SELECT * FROM funcionarios;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Adicionar Usuário Atual à Mapping Table
-- MAGIC Vamos adicionar o usuário atual à mapping table e ver o efeito.

-- COMMAND ----------

-- DBTITLE 1,Insert na Mapping Table
-- Adicionando o usuário atual à mapping table
INSERT INTO usuarios_acesso_especial 
SELECT CURRENT_USER(), 'Financeiro','123.456.789-10';

-- COMMAND ----------

-- DBTITLE 1,Select com linhas Filtradas
-- Verificando como o usuário atual vê os dados
SELECT * FROM funcionarios;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 6. Implementar Column Masking
-- MAGIC
-- MAGIC O Column Masking é implementado através de funções UDF específicas para cada coluna sensível.
-- MAGIC
-- MAGIC Implementamos duas máscaras:
-- MAGIC 1. `cpf_mask`: Mascara o CPF para todos os usuários
-- MAGIC 2. `salario_mask`: Mascara o salário para todos os usuários
-- MAGIC
-- MAGIC As funções de máscara consideram:
-- MAGIC - O departamento do usuário
-- MAGIC - Se o usuário está na tabela de mapeamento
-- MAGIC - Se o CPF corresponde ao do usuário

-- COMMAND ----------

-- DBTITLE 1,Criar funções UDF para máscaras
-- Criando funções UDF para cada máscara de coluna
CREATE OR REPLACE FUNCTION cpf_mask (cpf STRING)
RETURN CASE
  WHEN EXISTS(
    SELECT 1 FROM usuarios_acesso_especial u
    where u.email = CURRENT_USER()
  )
   AND cpf in (select cpf from usuarios_acesso_especial f where f.email = CURRENT_USER()) 
   THEN cpf
  ELSE '***.***.***-**'
END;

CREATE OR REPLACE FUNCTION salario_mask (salario DECIMAL , cpf STRING)
RETURN CASE
  WHEN EXISTS(
    SELECT 1 FROM usuarios_acesso_especial u
    where u.email = CURRENT_USER()
  )
   AND cpf in (select cpf from usuarios_acesso_especial f where f.email = CURRENT_USER()) 
   THEN salario
  ELSE 0
END;

-- COMMAND ----------

-- DBTITLE 1,Aplicar máscaras de colunas
-- Aplicando as máscaras às colunas
ALTER TABLE funcionarios ALTER COLUMN cpf SET MASK cpf_mask;
ALTER TABLE funcionarios ALTER COLUMN salario SET MASK salario_mask USING COLUMNS(cpf);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 7. Testar as Políticas
-- MAGIC
-- MAGIC Nesta seção, vamos verificar como as políticas de segurança afetam a visualização dos dados.
-- MAGIC Você poderá observar:
-- MAGIC - Apenas registros do seu departamento (devido ao RLS)
-- MAGIC - CPFs e salários mascarados para usuários sem permissão
-- MAGIC - Dados completos para usuários com acesso especial

-- COMMAND ----------

-- DBTITLE 1,Verificar dados após adicionar column MASKING
-- Verificando como o usuário vê os dados após ser adicionado à mapping table
SELECT * FROM funcionarios;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 8. Limpeza
-- MAGIC
-- MAGIC Por fim, realizamos a limpeza de todos os assets criados para evitar acúmulo de recursos.
-- MAGIC Esta é uma prática importante para manter o ambiente organizado e otimizado.

-- COMMAND ----------

-- DBTITLE 1,Remover assets
-- Removendo as máscaras e políticas
ALTER TABLE funcionarios ALTER COLUMN cpf DROP MASK;
ALTER TABLE funcionarios ALTER COLUMN salario DROP MASK;
ALTER TABLE funcionarios DROP ROW FILTER;

-- Removendo as funções UDF
DROP FUNCTION IF EXISTS acesso_especial_filter;
DROP FUNCTION IF EXISTS cpf_mask;
DROP FUNCTION IF EXISTS salario_mask;

-- Removendo as tabelas
DROP TABLE IF EXISTS funcionarios;
DROP TABLE IF EXISTS usuarios_acesso_especial; 