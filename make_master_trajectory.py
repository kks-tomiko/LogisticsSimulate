import json
import numpy as np
import os
import pandas as pd
import sqlite3
from typing import List


class ObjProp:
    def __init__(self, value: dict) -> None:
        # オブジェクト基本情報
        self.id = value["id"]
        self.name = value["name"]
        self.x_center = value["x_center"]
        self.y_center = value["y_center"]
        self.radius = value["radius"]
        self.splits = value["splits"]
        self.cycles = value["cycles"]
        self.priority_no = value["priority_no"]
        self.group_no = value["group_no"]
        self.barrier_range = value["barrier_range"]
        # 軌跡記録リスト
        self.x_position: List[float] = []
        self.y_position: List[float] = []
        self.step: List[int] = []

    # 軌跡記録
    def make_trajectory(self):
        # 円運動の軌跡を記録
        thetas = np.linspace(0, 2*self.cycles*np.pi, self.splits)
        for index, theta in enumerate(thetas):
            self.step.append(index)
            self.x_position.append(self.x_center + np.cos(self.radius*theta))
            self.y_position.append(self.y_center + np.sin(self.radius*theta))
        # pandas:物体の基本情報
        list_pre1 = [[self.id,
                      self.name,
                      self.x_center,
                      self.y_center,
                      self.radius,
                      self.splits,
                      self.cycles,
                      self.priority_no,
                      self.group_no,
                      self.barrier_range,
                    ]]
        columns1 = [
                    "obj_id",
                    "obj_name",
                    "x_center",
                    "y_center",
                    "radius",
                    "splits",
                    "cycles",
                    "priority_no",
                    "group_no",
                    "barrier_range",
                    ]
        # pandas:物体の軌跡情報
        # NOTE 縦持ちでデータフレーム作成するおまじない
        list_pre2 = zip(thetas, self.step, self.x_position, self.y_position)
        columns2 = [
                    "thetas",
                    "step",
                    "x_position",
                    "y_position",
                    ]
        self.df_pre1 = pd.DataFrame(list_pre1, columns=columns1)
        self.df_pre2 = pd.DataFrame(list_pre2, columns=columns2)
        # print(self.df_pre1.head(10))
        # print(self.df_pre2.head(10))

        # NOTE 物体の基本情報と軌跡情報を連結
        self.df = pd.concat([self.df_pre1, self.df_pre2], axis=1)
        # print(self.df.head(10))
        # NOTE 各項目のNULLを同じ値で上書き
        self.df.ffill(inplace=True)
        # print(self.df.head(10))

        return self.df


    # マスター軌跡データベース作成
    def write_sqlite(self, df: pd.DataFrame, name_db: str):
        file_sqlite3 = f"./db/{name_db}"
        conn = sqlite3.connect(file_sqlite3)
        df.to_sql(name_db, conn, if_exists='append', index=True)
        conn.close()


if __name__ == "__main__":
    # ■■■■■■■■■■■■■■■■■■■■■■■■■■■■
    # 前処理1
    # NOTE マスター更新する前に旧DBを削除
    str_name_db = "mst_obj_trajectory.db"
    if os.path.isfile(f"./db/{str_name_db}"):
        os.remove(f"./db/{str_name_db}")
    else:pass
    # ■■■■■■■■■■■■■■■■■■■■■■■■■■■■


    # ■■■■■■■■■■■■■■■■■■■■■■■■■■■■
    # 前処理2
    # NOTE JSONのオブジェクト情報を取得
    with open("./config/config_obj.json") as file:
        json_load = json.load(file)
        # print(json_load)
    # ■■■■■■■■■■■■■■■■■■■■■■■■■■■■


    # ■■■■■■■■■■■■■■■■■■■■■■■■■■■■
    # 本番処理
    # NOTE オブジェクトの軌跡を算出・DB書き込み
    list_obj:List[object] = []
    for key, value in json_load.items():
        # print(f"key:{key}")
        # print(f"value:{value}")
        obj = ObjProp(value)
        df_obj = obj.make_trajectory()
        obj.write_sqlite(df_obj, name_db=str_name_db)
    # ■■■■■■■■■■■■■■■■■■■■■■■■■■■■
