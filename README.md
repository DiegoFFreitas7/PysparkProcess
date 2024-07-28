
## Exemplo 
	python main.py gs://bucket/path_folder/public_condominios/* gs://bucket/path_folder/public_moradores/* gs://bucket/path_folder/public_imoveis/* gs://bucket/path_folder/public_transacoes/* gs://bucket/path_folder/aggregation

*O codigo busca os arquivos no formato avro no cloud storage

## dataStreaming_output.tar.gz
O arquivo "dataStreaming_output" contem os arquivos retornado pelo serviço datastream. Que é usado pelo codigo python para fazer o processamento
