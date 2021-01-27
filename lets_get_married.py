from sqlalchemy import create_engine
import pandas as pd
from pandas import ExcelWriter
from math import floor
from tqdm import tqdm
from collections import defaultdict
import re
import string
import argparse


def get_all_sections():
    Sections1 = """
    SELECT
        ds.original_fio,
        ds.id,
        ds.document_id, 
        ds.person_id, 
        dp.gender, 
        dd.income_year,
        dp.family_name,
        dp.name,
        dp.patronymic

    FROM declarations_section AS ds

    LEFT JOIN declarations_person AS dp 
        ON ds.person_id=dp.id

    LEFT JOIN declarations_document AS dd 
        ON ds.document_id=dd.id;
    """
    
    return pd.read_sql_query(Sections1, ENGINE)


def get_db(all_sections, person_type):
    Vehicle1 = f"""
    SELECT 
        section_id,
        COUNT(section_id) AS vehicle_num

    FROM declarations_vehicle

    WHERE 
        relative_id {person_type}

    GROUP BY
        section_id;
    """

    Realestate1 = f"""
    SELECT 
        section_id,
        SUM(square) AS realestate,
        COUNT(section_id) AS realestate_num

    FROM declarations_realestate

    WHERE 
        relative_id {person_type}

    GROUP BY
        section_id;
    """
    
    Income1 = f"""
    SELECT 
        section_id,
        SUM(size) AS income

    FROM declarations_income

    WHERE 
        relative_id {person_type}

    GROUP BY
        section_id;
    """
    
    vehicles_df = pd.read_sql_query(Vehicle1, ENGINE)
    realestates_df = pd.read_sql_query(Realestate1, ENGINE)
    incomes_df = pd.read_sql_query(Income1, ENGINE)
    
    all_sections.rename(columns={"id":"section_id"}, inplace=True)
    df = pd.merge(all_sections, vehicles_df, on="section_id", how="left")
    df = pd.merge(df, realestates_df, on="section_id", how="left")
    df = pd.merge(df, incomes_df, on="section_id", how="left")
    
    return df


def add_option_data(df, options_df, option, value, w):
    
    option_df = df[df[option] == value][["section_id", option]]
    option_df[option] = w
    
    if option_df.shape[0] > 0:
        options_df = pd.merge(
            options_df,
            option_df,
            on="section_id", 
            how="outer"
        )
    
    return options_df


def rounder(x):
    if x:
        return round(x)
    return x


def rounder_floor(x):
    if x:
        return floor(x/10)*10
    return x


def get_NAME(x):
    
    if x.person_id != 0:
        
        family_name = x["family_name"]
        name = x["name"][0]+"."
        
        patronymic = ""
        
        if x["patronymic"]:
            patronymic = x["patronymic"][0]+"."
        
        return f"{family_name} {name}{patronymic}".lower()
    
    return x.original_fio.lower()


def get_princes_and_princesses(df):
    return "\n".join([f"{i} {r.person_id} {r.NAME}" for i, r in df.iterrows()])


def main():
    all_sections = get_all_sections()
    spouses_data = get_db(all_sections, "= 2")
    self_data = get_db(all_sections, "IS NULL")

    spouses_data.dropna(
        subset=["vehicle_num", "realestate", "realestate_num", "income"], 
        how="all",
        inplace=True
    )

    spouses_data.dropna(
        subset=["realestate", "income"], 
        how="all",
        inplace=True
    )

    spouses_data.fillna(0, inplace=True)
    spouses_data["income_round"] = spouses_data.income.apply(rounder)
    spouses_data["realestate_round"] = spouses_data.realestate.apply(rounder)
    spouses_data["income_floor"] = spouses_data.income.apply(rounder_floor)
    spouses_data["realestate_floor"] = spouses_data.realestate.apply(rounder_floor)

    self_data.fillna(0, inplace=True)
    self_data["income_round"] = self_data.income.apply(rounder)
    self_data["realestate_round"] = self_data.realestate.apply(rounder)
    self_data["income_floor"] = self_data.income.apply(rounder_floor)
    self_data["realestate_floor"] = self_data.realestate.apply(rounder_floor)

    sp_df = spouses_data.set_index("section_id")
    se_df = self_data.set_index("section_id")

    ops = [
        "vehicle_num",
        "realestate",
        "realestate_num",
        "income",
        "income_round",
        "realestate_round",
        "income_floor",
        "realestate_floor"
    ]

    sp_id = sp_df.index.values

    sp_df.loc[sp_id, ops]+=se_df.loc[sp_id, ops]

    sp_df.gender = sp_df.gender.astype(str)
    sp_df.person_id = sp_df.person_id.astype(int)

    sp_df["NAME"] = sp_df.apply(get_NAME, axis=1)

    DATA = defaultdict(list)

    for year, group in sp_df.groupby("income_year"):
        print(year)
            
        group_sort = group.sort_values("gender", ascending=False)

        with tqdm(total=group_sort.shape[0]) as pbar:
            for i, r in group_sort.iterrows():

                if i in group_sort.index.values:

                    others = group_sort.drop(i)

                    if r.gender != "0":
                        others = others[others.gender != r.gender]

                    others = others[others.NAME != r.NAME]

                    ch = others[
                        (
                            (others.income == r.income)|
                            (others.income_round == r.income_round)|
                            (others.income_floor == r.income_floor)
                        )&
                        (others.realestate_num == r.realestate_num)&
                        (others.vehicle_num == r.vehicle_num)&
                        (
                            (others.realestate == r.realestate)|
                            (others.realestate_round == r.realestate_round)|
                            (others.realestate_floor == r.realestate_floor)
                        )
                    ]

                    if ch.shape[0] > 0:
                        DATA[year].append(
                            {
                                "section_id" : i,
                                "person_id" : r.person_id,
                                "name" : r.NAME,
                                "princes_and_princesses" : get_princes_and_princesses(ch)

                            }
                        )

                        if ch.shape[0] == 1:
                            group_sort.drop(ch.index.values, inplace=True)

                pbar.update()

    cols_for_wrap = ['princes_and_princesses']

    writer = ExcelWriter('davay_pozhenimsya.xlsx', engine='xlsxwriter')

    for k, v in DATA.items():
        df = pd.DataFrame(v)[["name", "section_id", "person_id", "princes_and_princesses"]]
        df.to_excel(writer, sheet_name=str(k), index=False)
            
        workbook  = writer.book
        worksheet = writer.sheets[str(k)]
        wrap_format = workbook.add_format({'text_wrap': True})

        d = dict(zip(range(26), list(string.ascii_uppercase)))

        for col in df.columns.get_indexer(cols_for_wrap):
            excel_header  =  d[col] + ':' + d[col]
            worksheet.set_column(excel_header, None, wrap_format)

    writer.save()


if __name__ == '__main__':
    argparser = argparse.ArgumentParser(description='⚭ ДАВАЙ ПОЖЕНИМСЯ! ⚭')
    argparser.add_argument('--user', type=str, required=True)
    argparser.add_argument('--password', type=str, required=True)
    argparser.add_argument('--server', type=str, required=True)

    args = argparser.parse_args()

    ENGINE = create_engine(f"mysql+pymysql://{args.user}:{args.password}@{args.server}")

    main()