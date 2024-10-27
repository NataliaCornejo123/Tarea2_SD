# Usa una imagen base de Python
FROM python:3.12-slim

# Establece el directorio de trabajo
WORKDIR /app

# Copia los archivos necesarios
COPY . /app

# Instala las dependencias
RUN pip install -r requirements.txt

# Expon el puerto en el que corre tu servidor gRPC
EXPOSE 50051

# Comando para ejecutar tu servidor
CMD ["python3", "Servidor.py"]
