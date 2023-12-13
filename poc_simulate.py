from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List
import datetime
import math
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation
import os
import pandas as pd
import sqlite3
import time
import uvicorn


# app = FastAPI()

# NOTE pandas表示オプションの変更
pd.set_option('display.max_rows', None)  # 行数を制限せず全て表示
# pd.set_option('display.max_columns', None)  # 列数を制限せず全て表示


# コンテキストマネージャを活用してデータベース接続を管理
class DatabaseTransaction:
    def __init__(self, dbname_trn: str):
        self.conn = sqlite3.connect(f"./db/{dbname_trn}", check_same_thread=False)
        self.cursor = self.conn.cursor()

    def __enter__(self):
        return self.cursor

    def __exit__(self, exc_type, exc_value, traceback):
        try:
            self.conn.commit()
        except:
            self.conn.rollback()
        finally:
            self.conn.close()


# 実績書き込み用 テーブル定義＆作成SQL
def create_database_transaction(dbname_trn: str) -> None:
    with DatabaseTransaction(dbname_trn) as cursor:
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS trn_obj_trajectory (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at TEXT not null UNIQUE,
                obj_id INTEGER not null,
                obj_name TEXT not null,
                priority_no INTEGER not null,
                group_no INTEGER not null,
                barrier_range REAL not null,
                step INTEGER not null,
                x_position  REAL not null,
                y_position  REAL not null,
                present_location INTEGER not null
            )
        ''')


# マスタDBから情報取得
def read_sqlite(dbname_mst: str) -> pd.DataFrame:
    file_sqlite3 = f"./db/{dbname_mst}"
    conn = sqlite3.connect(file_sqlite3)

    str_query = '''
                SELECT
                    obj_id,
                    obj_name,
                    x_center,
                    y_center,
                    radius,
                    priority_no,
                    group_no,
                    barrier_range,
                    step,
                    x_position,
                    y_position
                FROM 'mst_obj_trajectory.db'
                ORDER BY obj_id, step;
                '''

    df_from_mst = pd.read_sql_query(str_query, conn)
    conn.close()
    # print(df_from_mst)

    df_current_flg = pd.DataFrame({
                                    'current_flg':[pd.NA],
                                    'created_at':pd.NaT
                                    },
                                  )
    df_concat = pd.concat([df_from_mst,df_current_flg], axis=1)
    # print(df_concat.info())
    df_pre = df_concat.fillna({
                    'current_flg':0,
                    })

    # 特定の条件を持つ行の フラグ、datetime 値を更新
    condition = df_pre['step'] == 0
    # 使い方：loc[行,列]
    # NOTE なぜかquery()では失敗する。別オブジェクト参照するのか？放置。
    # df_pre.query("step==0").loc[:, 'current_flg'] = 1
    # print(df_pre.query("step==0").loc[:, 'current_flg'])
    # df_pre.query("step==0").loc[:, 'created_at'] = datetime.datetime.now()
    df_pre.loc[condition, 'current_flg'] = 1
    df_pre.loc[condition, 'created_at'] = datetime.datetime.now()
    # print(df_pre)
    # print(df_pre.info())
    return df_pre


# NOTE ビジネスロジック
def analysis(df_pre: pd.DataFrame) -> pd.DataFrame:
    print(df_pre)
    # print(df_pre.info())
    # ループ処理
    for step in range(len(df_pre['step'])):
        # 処理日時分秒取得
        nowt = datetime.datetime.now()
        # objectごとにフラグのあるステップ番号を抽出
        df_1 = df_pre.query(f'obj_id==1 & current_flg==1')
        df_2 = df_pre.query(f'obj_id==2 & current_flg==1')
        if df_1.empty or df_2.empty:
            break
        df1_step = df_1['step'].values[0]
        df1_step2 = df1_step + 1
        df2_step = df_2['step'].values[0]
        df2_step2 = df2_step + 1
        print(f"df_1['step']: {df_1['step']}")
        print(f"df_2['step']: {df_2['step']}")
        # 干渉壁の和
        sum_barrier_range = df_1['barrier_range'].values[0] + df_2['barrier_range'].values[0]
        # object間の距離算出
        x_diff = df_2['x_position'].values[0] - df_1['x_position'].values[0]
        # print(f"x_diff:{x_diff}")
        y_diff = df_2['y_position'].values[0] - df_1['y_position'].values[0]
        # print(f"y_diff:{y_diff}")
        distance_obj = math.sqrt((x_diff)**2 + (y_diff)**2)
        # print(distance_obj)
        # 干渉しないので、df_1もdf_2も歩進する場合
        # df_1とdf_2のフラグを持つステップが異なるので、それぞれ歩進しないとダメ
        if sum_barrier_range < distance_obj:
            condition = (df_pre['priority_no'] == 1) & (df_pre['step'] == df1_step) & (df_pre['current_flg'] == 1)
            condition2 = (df_pre['priority_no'] == 1) & (df_pre['step'] == df1_step2) & (df_pre['current_flg'] == 0)
            df_pre.loc[condition, 'current_flg'] = 0
            df_pre.loc[condition2, 'current_flg'] = 1
            df_pre.loc[condition2, 'created_at'] = nowt
            condition = (df_pre['priority_no'] == 2) & (df_pre['step'] == df2_step) & (df_pre['current_flg'] == 1)
            condition2 = (df_pre['priority_no'] == 2) & (df_pre['step'] == df2_step2) & (df_pre['current_flg'] == 0)
            df_pre.loc[condition, 'current_flg'] = 0
            df_pre.loc[condition2, 'current_flg'] = 1
            df_pre.loc[condition2, 'created_at'] = nowt
        else:
            # どれでもいい。暫定でこれにしてる。
            if df1_step2 >= 100:
                break
            else:
                # df_1を優先する場合
                if df_1['priority_no'].values[0] < df_2['priority_no'].values[0]:
                    df_1 = df_pre.query(f"obj_id==1 & step=={df1_step2}")
                    x_diff = df_2['x_position'].values[0] - df_1['x_position'].values[0]
                    y_diff = df_2['y_position'].values[0] - df_1['y_position'].values[0]
                    distance_obj = math.sqrt((x_diff)**2 + (y_diff)**2)
                    # STEP[N+1]をチェックした結果、優先度1を歩進してOKだった場合
                    if (sum_barrier_range / distance_obj) < 2:
                        condition = (df_pre['priority_no'] == 1) & (df_pre['step'] == df1_step) & (df_pre['current_flg'] == 1)
                        condition2 = (df_pre['priority_no'] == 1) & (df_pre['step'] == df1_step2) & (df_pre['current_flg'] == 0)
                        df_pre.loc[condition, 'current_flg'] = 0
                        df_pre.loc[condition2, 'current_flg'] = 1
                        df_pre.loc[condition2, 'created_at'] = nowt
                        # print(df_pre.loc[condition, 'current_flg'])
                        # print(df_pre.loc[condition2, 'current_flg'])
                        # print(df_pre.loc[condition2, 'created_at'])
                    # STEP[N+1]をチェックした結果、優先度1を歩進してNGだった場合。衝突するので優先度2を歩進する。
                    else:
                        condition = (df_pre['priority_no'] == 2) & (df_pre['step'] == df2_step) & (df_pre['current_flg'] == 1)
                        condition2 = (df_pre['priority_no'] == 2) & (df_pre['step'] == df2_step2) & (df_pre['current_flg'] == 0)
                        df_pre.loc[condition, 'current_flg'] = 0
                        df_pre.loc[condition2, 'current_flg'] = 1
                        df_pre.loc[condition2, 'created_at'] = nowt
                # df_2を優先する場合
                else:
                    df_2 = df_pre.query(f"obj_id==2 & step=={df2_step2}")
                    x_diff = df_2['x_position'].values[0] - df_1['x_position'].values[0]
                    y_diff = df_2['y_position'].values[0] - df_1['y_position'].values[0]
                    distance_obj = math.sqrt((x_diff)**2 + (y_diff)**2)
                    # STEP[N+1]をチェックした結果、優先度2を歩進してOKだった場合
                    if (sum_barrier_range / distance_obj) < 2:
                        condition = (df_pre['priority_no'] == 2) & (df_pre['step'] == df2_step) & (df_pre['current_flg'] == 1)
                        condition2 = (df_pre['priority_no'] == 2) & (df_pre['step'] == df2_step2) & (df_pre['current_flg'] == 0)
                        df_pre.loc[condition, 'current_flg'] = 0
                        df_pre.loc[condition2, 'current_flg'] = 1
                        df_pre.loc[condition2, 'created_at'] = nowt
                    # STEP[N+1]をチェックした結果、優先度2を歩進してNGだった場合。衝突するので優先度1を歩進する。
                    else:
                        condition = (df_pre['priority_no'] == 1) & (df_pre['step'] == df1_step) & (df_pre['current_flg'] == 1)
                        condition2 = (df_pre['priority_no'] == 1) & (df_pre['step'] == df1_step2) & (df_pre['current_flg'] == 0)
                        df_pre.loc[condition, 'current_flg'] = 0
                        df_pre.loc[condition2, 'current_flg'] = 1
                        df_pre.loc[condition2, 'created_at'] = nowt
        time.sleep(0.1)
    return df_pre


def animation(df_trn):
    # NaTレコード削除
    df_trn.dropna(inplace=True)
    # print(df_trn)

    # プロット初期化
    figure, ax = plt.subplots()
    # plotのLine2Dobjectは、戻り値がリスト型となる。
    lines = ax.plot([], [], marker='o', label='Object 1')
    lines += ax.plot([], [], marker='s', label='Object 2')

    # X/Y軸の範囲を可変設定
    float_xlim_max = df_trn['x_position'].max()
    float_xlim_min = df_trn['x_position'].min()
    float_ylim_max = df_trn['y_position'].max()
    float_ylim_min = df_trn['y_position'].min()
    limit = max(abs(float_xlim_min), abs(float_xlim_max), abs(float_ylim_min), abs(float_ylim_max))

    plt.xlim(-1*math.ceil(limit)*1.1, math.ceil(limit)*1.1)
    plt.ylim(-1*math.ceil(limit)*1.1, math.ceil(limit)*1.1)

    # プロット更新関数
    start_time = time.time()
    def update(frame):
        # 時間軸を比較するためにはクォーテーションを2重化する。loc[]は使わず、query()で絞る。
        # ゆくゆくは汎用化したいが、まずは手作業で作る
        x1 = df_trn.query(f"obj_id==1 & created_at<='{frame}'").loc[:, 'x_position']
        y1 = df_trn.query(f"obj_id==1 & created_at<='{frame}'").loc[:, 'y_position']
        x2 = df_trn.query(f"obj_id==2 & created_at<='{frame}'").loc[:, 'x_position']
        y2 = df_trn.query(f"obj_id==2 & created_at<='{frame}'").loc[:, 'y_position']
        lines[0].set_data(x1,y1)
        lines[1].set_data(x2,y2)
        total_time = time.time() - start_time
        figure.suptitle(f'Time Chart : {total_time:.1f} s')
        return lines

    # アニメーション作成
    # framesはINTEGERを受け付ける。len(df_trn)でもよいが、指定したカラムでもソート可能。
    animation = FuncAnimation(fig=figure, func=update, frames=df_trn['created_at'].unique(), interval=100)

    # 凡例の表示
    ax.legend()

    animation.save("test.gif", writer='imagemagick')
    plt.show()
    plt.close()


if __name__ == "__main__":
    # ■■■■■■■■■■■■■■■■■■■■■■■■■■■■
    # 前処理1
    # NOTE 実行前に前回の実績DBを削除
    name_trn_db = "trn_obj_trajectory.db"
    if os.path.isfile(f"./db/{name_trn_db}"):
        os.remove(f"./db/{name_trn_db}")
    else:pass
    # NOTE 実績DB新規作成
    create_database_transaction(name_trn_db)
    # ■■■■■■■■■■■■■■■■■■■■■■■■■■■■


    # ■■■■■■■■■■■■■■■■■■■■■■■■■■■■
    # 前処理2
    # NOTE マスタDBを取得＋加工して用意する。これで準備完了。本番処理へ。
    name_mst_db = "mst_obj_trajectory.db"
    df_pre = read_sqlite(name_mst_db)
    # ■■■■■■■■■■■■■■■■■■■■■■■■■■■■


    # ■■■■■■■■■■■■■■■■■■■■■■■■■■■■
    # 本番処理
    # NOTE ビジネスロジック。
    df_trn = analysis(df_pre)
    print(df_trn)
    # NOTE アニメーション表示。
    animation(df_trn)
    # ■■■■■■■■■■■■■■■■■■■■■■■■■■■■


    # NOTE 絶対パスを渡さないとリロードができない
    # uvicorn.run("poc_simulate:app", host='127.0.0.1', port=8000, reload=True)
