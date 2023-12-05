from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import datetime
import pandas
import sqlite3
import uvicorn
from make_master_trajectory import ObjProp


app = FastAPI()


# NOTE コンテキストマネージャを活用してデータベース接続を管理
class DatabaseMaster:
    def __init__(self):
        self.conn = sqlite3.connect('mst_obj_trajectory.db', check_same_thread=False)
        self.cursor = self.conn.cursor()

    def __enter__(self):
        return self.cursor

    def __exit__(self, exc_type, exc_value, traceback):
        try:
            self.conn.commit()
        finally:
            self.conn.rollback()
            self.conn.close()


# コンテキストマネージャを活用してデータベース接続を管理
class DatabaseTransaction:
    def __init__(self):
        self.conn = sqlite3.connect('trn_obj_trajectory.db', check_same_thread=False)
        self.cursor = self.conn.cursor()

    def __enter__(self):
        return self.cursor

    def __exit__(self, exc_type, exc_value, traceback):
        try:
            self.conn.commit()
        finally:
            self.conn.rollback()
            self.conn.close()


# 物体位置情報テーブルの作成
def create_database_transaction() -> None:
    with DatabaseTransaction() as cursor:
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS trn_obj_trajectory (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at TEXT not null UNIQUE,
                obj_name TEXT not null,
                priority_no INTEGER not null,
                x_cordinate  REAL not null,
                y_cordinate  REAL not null
            )
        ''')


# pydanticモデルを使用してリクエストボディの検証
class UpdateMaster(BaseModel):
    created_at: datetime.datetime
    obj_name: str
    priority_no: int
    x_cordinate: float
    y_cordinate: float


# pydanticモデルを使用してリクエストボディの検証
class UpdateTransaction(BaseModel):
    created_at: datetime.datetime
    obj_name: str
    priority_no: int
    x_cordinate: float
    y_cordinate: float


# FastAPIのエンドポイント（非同期化）
@app.get("/update_master/")
def make_master():
    try:
        obj_A = ObjProp(id=1, name='obj_A', center=(0, 0), radius=2, splits=100, priority_no=1, group_no=1)
        obj_B = ObjProp(id=2, name='obj_B', center=(1, 1), radius=1, splits=100, priority_no=2, group_no=1)

        mst_trj_A = obj_A.make_trajectory()
        mst_trj_B = obj_B.make_trajectory()

        with DatabaseMaster() as db:
            # 位置情報をデータベースに挿入
            db.execute(
                """
                INSERT INTO mst_obj_trajectory (
                    obj_id,
                    obj_name,
                    x_center,
                    y_center,
                    radius,
                    splits,
                    priority_no,
                    group_no,
                    step,
                    x_trajectory,
                    y_trajectory)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (mst_trj_A.obj_id,
                 mst_trj_A.obj_name,
                 mst_trj_A.x_center,
                 mst_trj_A.y_center,
                 mst_trj_A.radius,
                 mst_trj_A.splits,
                 mst_trj_A.priority_no,
                 mst_trj_A.group_no,
                 mst_trj_A.step,
                 mst_trj_A.x_trajectory,
                 mst_trj_A.y_trajectory)
            )

        return {"message": "Master updated successfully"}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# FastAPIのエンドポイント（非同期化）
@app.post("/update_transaction/")
async def update_position(position: UpdateTransaction):
    try:
        with DatabaseTransaction() as db:
            # 位置情報をデータベースに挿入
            db.execute(
                """
                INSERT INTO trn_obj_trajectory (created_at, obj_name, priority_no, x_cordinate, y_cordinate)
                VALUES (?, ?, ?, ?, ?)
                """,
                (position.created_at, position.obj_name, position.priority_no, position.x_cordinate, position.y_cordinate)
            )

        return {"message": "Transaction updated successfully"}

    except Exception as e:
        db.conn.rollback()
        db.conn.close()
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    # NOTE 起動時にデータベース作成チェックが必要
    create_database_transaction()

    # NOTE 絶対パスを渡さないとリロードができない
    uvicorn.run("poc_simulate:app", host='127.0.0.1', port=8000, reload=True)
