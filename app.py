from flask import Flask, redirect, render_template, request
import pandas as pd
from db import get_db_connection
import os
import threading
import time




app = Flask(__name__)

CSV_RUTA = r'C:\Carga\datos4.csv'  # ← Cambia esta ruta a la tuya


@app.route('/')
def index():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute('SELECT * FROM miapp.tbl_cuota')
    datos = cur.fetchall()
    conn.close()
    return render_template('index.html', datos=datos)

@app.route('/editar/<int:id>', methods=['GET', 'POST'])
def editar(id):
    conn = get_db_connection()
    cur = conn.cursor()

    if request.method == 'POST':
        Mes = request.form['Mes']
        Periodos = request.form['Periodos']
        cliente = request.form['Cliente']
        Rif = request.form['Rif']
        Codigo = request.form['codigo']
        Organizacion = request.form['organizacion']
        Producto = request.form['producto']
        Canal = request.form['Canal']
        Canal_Normalizado = request.form['canal normalizado']
        Clase_de_Producto = request.form['Clase de Producto']
        Cuota = request.form['cuota']
        TIPO_CANAL = request.form['tipo canal']
        Razón_Social_Normalizado = request.form['razon social']
        ce = request.form['CE']
        Sucursal_conf = request.form['Sucursal conf']
        cur.execute("""UPDATE miapp.tbl_cuota SET "Mes"=%s, "Periodo"=%s, "Cliente"=%s, "Rif"=%s, "CodigoProducto"=%s, "Organizacion"=%s, "Producto"=%s, "Canal"=%s, "CanalNormalizado"=%s, "ClaseProducto"=%s, "Cuota"=%s, "TIPOCANAL"=%s, "RazonSocial"=%s, "ce"=%s, "Sucursalconf"=%s WHERE id=%s""", (Mes, Periodos, cliente, Rif, Codigo, Organizacion, Producto, Canal, Canal_Normalizado, Clase_de_Producto, Cuota, TIPO_CANAL, Razón_Social_Normalizado, ce, Sucursal_conf, id))
        conn.commit()
        conn.close()
        return redirect('/')

    cur.execute('SELECT * FROM miapp.tbl_cuota WHERE id = %s', (id,))
    dato = cur.fetchone()
    conn.close()
    return render_template('edit.html', dato=dato)

# Función automática para importar CSV cada 10 minutos
def importar_csv_automaticamente():
    while True:
        try:
            if os.path.exists(CSV_RUTA):
                df = pd.read_csv(CSV_RUTA)
                conn = get_db_connection()
                cur = conn.cursor()

                cur.execute('DELETE FROM miapp.tbl_cuota')

                for _, row in df.iterrows():
                    cur.execute(""" INSERT INTO miapp.tbl_cuota ( "Mes", "Periodo", "Cliente", "Rif", "CodigoProducto", "Organizacion", "Producto","Canal", "CanalNormalizado", "ClaseProducto", "Cuota", "TIPOCANAL","RazonSocial", "ce", "Sucursalconf") VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""", (row['Mes'], row['Periodo'], row['Cliente'], row['Rif'], row['CodigoProducto'], row['Organizacion'], row['Producto'], row['Canal'], row['CanalNormalizado'],    row['ClaseProducto'], row['Cuota'], row['TIPOCANAL'], row['RazonSocial'],row['ce'], row['Sucursalconf']))

                   
                conn.commit()
                conn.close()
                print("CSV importado correctamente.")
            else:
                print(f"Archivo no encontrado en {CSV_RUTA}")
        except Exception as e:
            print("Error al importar CSV:", e)

        time.sleep(600)  # Espera 10 minutos

# Iniciar hilo en segundo plano al arrancar
hilo_importador = threading.Thread(target=importar_csv_automaticamente, daemon=True)
hilo_importador.start()

if __name__ == '__main__':
    
    app.run(debug=True)