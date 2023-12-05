import numpy as np
import pandas as pd
import sqlite3
from typing import List


class ObjProp:
    def __init__(self, id: int, name: str, center: tuple, radius: float, splits: float, priority_no: int, group_no: int) -> None:
        self.id = id
        self.name = name
        self.center = center
        self.radius = radius
        self.splits = splits
        self.priority_no = priority_no
        self.group_no = group_no
        # 軌跡記録リスト
        self.x_trajectory: List[float] = []
        self.y_trajectory: List[float] = []
        self.step: List[int] = []

    def make_trajectory(self):
        # リストtheta作成
        thetas = np.linspace(0, 2*np.pi, self.splits)
        for index, theta in enumerate(thetas):
            self.step.append(index)
            self.x_trajectory.append(self.center[0] + np.cos(self.radius*theta))
            self.y_trajectory.append(self.center[1] + np.sin(self.radius*theta))
        # pandas:物体の基本情報
        list_pre1 = [[self.id,
                      self.name,
                      self.center[0],
                      self.center[1],
                      self.radius,
                      self.splits,
                      self.priority_no,
                      self.group_no,
                    ]]
        columns1 = [
                    "obj_id",
                    "obj_name",
                    "x_center",
                    "y_center",
                    "radius",
                    "splits",
                    "priority_no",
                    "group_no",
                    ]
        # pandas:物体の軌跡情報
        # NOTE 縦持ちでデータフレーム作成するおまじない
        list_pre2 = zip(self.step, self.x_trajectory, self.y_trajectory)
        columns2 = [
                    "step",
                    "x_trajectory",
                    "y_trajectory",
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

        # master_trajectory データフレーム完成
        return self.df


def df_merge(obj1: pd.DataFrame, obj2: pd.DataFrame):
    merge_obj = pd.concat([obj1, obj2], axis=0)

    return merge_obj


def to_sqlite(df: pd.DataFrame, name_db: str):
    file_sqlite3 = f"./db/{name_db}.db"
    conn = sqlite3.connect(file_sqlite3)
    df.to_sql(name_db, conn, if_exists='replace', index=True)
    conn.close()


if __name__ == "__main__":
    # NOTE いずれ物体基本情報リストを作ろう
    # 起動時にデータベース作成チェックが必要
    obj_A = ObjProp(id=1, name='obj_A', center=(0, 0), radius=2, splits=100, priority_no=1, group_no=1)
    obj_B = ObjProp(id=2, name='obj_B', center=(1, 1), radius=1, splits=100, priority_no=2, group_no=1)
    df_obj_A = obj_A.make_trajectory()
    df_obj_B = obj_B.make_trajectory()

    df_merged = df_merge(df_obj_A, df_obj_B)

    to_sqlite(df=df_merged, name_db= "mst_obj_trajectory")
