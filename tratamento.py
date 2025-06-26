# Instala o PySpark (necessário apenas em ambientes como Google Colab)
!pip install pyspark

# Importações principais do PySpark
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, when
from pyspark.sql.functions import to_date, year
from pyspark.sql.types import IntegerType

# Inicia a sessão do Spark
spark = SparkSession.builder.getOrCreate()

# Leitura do arquivo CSV com estatísticas de vídeos
df_video = spark.read.csv(
    '/content/drive/MyDrive/data/Material/videos-stats.csv',
    header=True,
    inferSchema=True
)

# Preenche valores nulos nas colunas Likes, Comments e Views com 0
df_video = df_video.fillna({'Likes': 0, 'Comments': 0, 'Views': 0})

# Leitura do arquivo CSV com comentários
df_comments = spark.read.csv(
    '/content/drive/MyDrive/data/Material/comments.csv',
    header=True,
    inferSchema=True
)

# Exibe a quantidade de registros antes da limpeza
print(f'Quantidade de registros de vídeos: {df_video.count()}')
print(f'Quantidade de registros de comentários: {df_comments.count()}')

# Remove registros com 'Video ID' nulo
df_video = df_video.filter(df_video['Video ID'].isNotNull())
df_comments = df_comments.filter(df_comments['Video ID'].isNotNull())

# Exibe a quantidade de registros após remoção dos nulos
print(f'Quantidade de registros de vídeos: {df_video.count()}')
print(f'Quantidade de registros de comentários: {df_comments.count()}')

# Remove vídeos com 'Video ID' duplicados
df_video = df_video.dropDuplicates(['Video ID'])

# Converte as colunas Likes, Comments e Views para o tipo inteiro
df_video = df_video.withColumn("Likes", df_video["Likes"].cast(IntegerType())) \
                   .withColumn("Comments", df_video["Comments"].cast(IntegerType())) \
                   .withColumn("Views", df_video["Views"].cast(IntegerType()))

# Cria a coluna 'Interaction' com a soma de Likes, Comments e Views
df_video = df_video.withColumn(
    'Interaction',
    col('Likes') + col('Comments') + col('Views')
)

# Converte a coluna 'Published At' para o tipo data (esperando formato 'dd-MM-yyyy')
df_video = df_video.withColumn('Published At', to_date(col('Published At'), 'dd-MM-yyyy'))

# Cria a coluna 'Year' extraída da data de publicação
df_video = df_video.withColumn('Year', year(col('Published At')))

# Junta os dados de vídeos com os comentários pelo campo 'Video ID'
df_join_video_comments = df_video.join(df_comments, on='Video ID', how='inner')
df_join_video_comments.show()

# Lê outro conjunto de dados com vídeos dos EUA
df_us_video = spark.read.csv(
    '/content/drive/MyDrive/data/Material/USvideos.csv',
    header=True,
    inferSchema=True
)

# Junta os vídeos com os vídeos dos EUA com base no título (Title)
df_join_video_us = df_video.join(df_us_video, on='Title', how='inner')
df_join_video_us.show()

# Verifica a quantidade de valores NÃO nulos em cada coluna do df_video
df_video.select([
    _sum(when(col(c).isNotNull(), 1).otherwise(0)).alias(c)
    for c in df_video.columns
]).show()

# Remove a coluna '_c0' do df_video
df_video = df_video.drop('_c0')

# Salva df_video no formato Parquet, com cabeçalho (esquema é incluído automaticamente)
df_video.write.mode('overwrite').option('header', True).parquet('/content/videos-tratados-parquet')

# Remove a coluna '_c0' do df_join_video_comments (caso exista)
df_join_video_comments = df_join_video_comments.drop('_c0')

# Salva o dataframe combinado de vídeos e comentários em Parquet
df_join_video_comments.write.mode('overwrite').option('header', True).parquet('/content/videos-comments-tratados-parquet')
