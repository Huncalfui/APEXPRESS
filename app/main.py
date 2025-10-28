from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Optional
import os
import psycopg
from psycopg.rows import dict_row

DATABASE_URL = os.getenv('DATABASE_URL')
app = FastAPI(title="ApetitoX Inventario API", version="0.2.1")

def get_conn():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL no configurada")
    return psycopg.connect(DATABASE_URL)

class Ingreso(BaseModel):
    material_sku: str
    cantidad: float
    costo_unit: float
    referencia: Optional[str] = None
    user_id: Optional[str] = None

class Lote(BaseModel):
    producto_sku: str
    cantidad_producida: float
    merma: float = 0.0
    lote: str
    user_id: Optional[str] = None

@app.post("/inventory/ingreso")
def registrar_ingreso(payload: Ingreso):
    with get_conn() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                "select id, coalesce(stock_actual,0) as stock_actual, coalesce(avg_cost,0) as avg_cost "
                "from materials where sku=%s for update",
                (payload.material_sku,)
            )
            mat = cur.fetchone()
            if not mat: raise HTTPException(404, "Material no encontrado")

            cur.execute(
                """
                insert into inventory_movements(material_id, tipo, cantidad, costo_unit, origen, referencia, user_id)
                values (%s, 'IN', %s, %s, 'compra', %s, %s)
                returning id
                """,
                (mat['id'], payload.cantidad, payload.costo_unit, payload.referencia, payload.user_id)
            )
            mov_id = cur.fetchone()['id']

            sa=float(mat['stock_actual']); ca=float(mat['avg_cost'])
            qi=float(payload.cantidad);    ci=float(payload.costo_unit)
            nuevo_stock = sa + qi
            nuevo_avg = 0 if nuevo_stock <= 0 else ((sa*ca)+(qi*ci))/nuevo_stock

            cur.execute(
                "update materials set stock_actual=%s, avg_cost=%s where id=%s",
                (nuevo_stock, nuevo_avg, mat['id'])
            )
    return {"ok": True, "mov_id": str(mov_id)}

@app.post("/production/lote")
def registrar_lote(payload: Lote):
    with get_conn() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute("select id from products where sku=%s and is_active=true", (payload.producto_sku,))
            prod = cur.fetchone()
            if not prod: raise HTTPException(404, "Producto no encontrado")

            cur.execute("""
                insert into production_batches(producto_id, lote, cantidad_producida, merma, user_id)
                values (%s, %s, %s, %s, %s) returning id
            """, (prod['id'], payload.lote, payload.cantidad_producida, payload.merma, payload.user_id))
            lote_id = cur.fetchone()['id']

            cur.execute("select material_id, qty_por_unidad from bom where producto_id=%s", (prod['id'],))
            bom_rows = cur.fetchall()

            for row in bom_rows:
                cur.execute("select id, coalesce(avg_cost,0) as avg_cost, coalesce(stock_actual,0) as stock_actual "
                            "from materials where id=%s for update", (row['material_id'],))
                matc = cur.fetchone()
                consumo = float(row['qty_por_unidad']) * float(payload.cantidad_producida)

                cur.execute("""
                    insert into inventory_movements(material_id, tipo, cantidad, costo_unit, origen, referencia, user_id)
                    values (%s, 'OUT', %s, %s, 'produccion', %s, %s)
                """, (row['material_id'], consumo, matc['avg_cost'], payload.lote, payload.user_id))

                cur.execute("update materials set stock_actual=%s where id=%s",
                            (float(matc['stock_actual']) - consumo, row['material_id']))
    return {"ok": True, "lote_id": str(lote_id)}

@app.get("/inventory/stock")
def consultar_stock(material_sku: str):
    with get_conn() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute("select sku, name, unidad, stock_actual, avg_cost from materials where sku=%s",
                        (material_sku,))
            mat = cur.fetchone()
            if not mat: raise HTTPException(404, "Material no encontrado")
            return mat

@app.get("/reports/kardex")
def kardex(material_sku: str, desde: Optional[str] = None, hasta: Optional[str] = None):
    qry = [
        "select im.created_at, im.tipo, im.cantidad, im.costo_unit, im.origen, im.referencia",
        "from inventory_movements im join materials m on m.id=im.material_id",
        "where m.sku=%s"
    ]
    params = [material_sku]
    if desde: qry.append("and im.created_at >= %s"); params.append(desde)
    if hasta: qry.append("and im.created_at <= %s"); params.append(hasta)
    qry.append("order by im.created_at asc")
    with get_conn() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute("\n".join(qry), params)
            return {"material": material_sku, "movimientos": cur.fetchall()}
- name: Railway Deploy
  uses: bervProject/railway-deploy@0.1.2-beta
