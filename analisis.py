from elasticsearch import Elasticsearch
import matplotlib.pyplot as plt
from datetime import datetime
import matplotlib.dates as mdates


es = Elasticsearch([{'scheme': 'http', 'host': 'elasticsearch', 'port': 9200}])

def fetch_metrics():
    # Consultar datos de Elasticsearch
    response = es.search(
        index='order_metrics',
        body={'size': 1000, 'query': {'match_all': {}}}
    )
    return response['hits']['hits']

def analyze_data(data):
    states = []
    timestamps = []
    
    for entry in data:
        source = entry['_source']
        
        if 'state' in source and 'timestamp' in source:
            states.append(source['state'])
            
            timestamps.append(datetime.fromisoformat(source['timestamp'].replace('Z', '+00:00')))
    
   
    state_to_numeric = {state: index for index, state in enumerate(set(states))}
    numeric_states = [state_to_numeric[state] for state in states]

    # Graficar
    plt.figure(figsize=(10, 5))
    plt.plot(timestamps, numeric_states, marker='o')
    
    # Formatear el eje X
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d %H:%M'))
    plt.gca().xaxis.set_major_locator(mdates.HourLocator(interval=1))
    plt.gcf().autofmt_xdate()  # Rotar las etiquetas del eje X

    plt.xlabel('Timestamp')
    plt.ylabel('Estados')
    plt.title('Cambio de estado de los pedidos en el tiempo')
    
    # Usar una leyenda para mostrar los estados
    plt.yticks(list(state_to_numeric.values()), list(state_to_numeric.keys()))
    
    plt.show()

if __name__ == '__main__':
    try:
        data = fetch_metrics()
        analyze_data(data)
    except Exception as e:
        print("Error al recuperar o analizar los datos:", e)

