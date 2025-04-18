"""
Import sales mix and export menu engineering report to excel
"""

import argparse
import os
import re

import numpy as np
import openpyxl
import pandas as pd
from openpyxl.styles import NamedStyle


def removedups(x):
    """Turn the list into a dict then back to a list to remove duplicates"""
    return list(dict.fromkeys(x))


def engineer(df):
    """Assigns bool if less than mean for Quantity and Margin"""
    qtm = df["Qty"].mean()
    df["qty_mn"] = np.where(df.Qty < qtm, False, True)
    mrgn = df["Margin"].mean()
    df["mrg_mn"] = np.where(df.Margin < mrgn, False, True)
    return df


def rating(df):
    """Assigns rating to each menu item"""
    if df["qty_mn"] is True:
        if df["mrg_mn"] is True:
            return "Star"
        else:
            return "Opportunity"
    if df["qty_mn"] is False:
        if df["mrg_mn"] is True:
            return "Puzzle"
        else:
            return "Dog"


def make_dataframe(catg, product_mix):
    """Separates each store into it's own dataframe"""
    df = product_mix.drop(product_mix[product_mix.Textbox27 != catg].index)
    return df


def make_dataframe1(catg, menu_analysis):
    """Separates each store into it's own dataframe"""
    df = menu_analysis.drop(menu_analysis[menu_analysis.Location != catg].index)
    return df


def menucatagory(catg, df_menu):
    """Create a separate dataframe for each menu category"""
    df = df_menu.drop(df_menu[df_menu.Cat2 != catg].index)
    if df.empty:
        print(f"{catg} is empty")
    return df


def removeSpecial(df):
    """Removes specialty items from the dataframes"""
    with open("./specialty.txt") as file:
        specialty_patterns = file.read().split("\n")

    for pattern in specialty_patterns:
        df = df.drop(df[df.MenuItem == pattern].index)

    # with open("./regex_list.txt") as file:
    #     regex_patterns = file.read().split("\n")

    # Print the regex patterns and the number of rows in df
    #    print("Regex patterns:", regex_patterns)
    #    print("Number of rows in df:", len(df))
    #    pause = input("Press enter to continue")

    #    for pattern in regex_patterns:
    #        print("current pattern:", pattern)
    #        df = df.drop(df[df.MenuItem.str.contains(f'{pattern}', na=False, regex=True)].index)
    #        print("Number of matched rows:", len(df[df.MenuItem.str.contains(f'{pattern}', na=False, regex=True)]))

    df = df.drop(df[df.MenuItem.str.contains(r"^No ", na=False, regex=True)].index)
    df = df.drop(df[df.MenuItem.str.contains(r" Only$", na=False, regex=True)].index)
    df = df.drop(df[df.MenuItem.str.contains(r"^& ", na=False, regex=True)].index)
    df = df.drop(df[df.MenuItem.str.contains(r"^Seat ", na=False, regex=True)].index)
    df = df.drop(df[df.MenuItem.str.contains(r"Allergy$", na=False, regex=True)].index)
    df = df.drop(
        df[df.MenuItem.str.contains(r"for Salad.*", na=False, regex=True)].index
    )
    df = df.drop(
        df[df.MenuItem.str.contains(r".*for Steak.*", na=False, regex=True)].index
    )
    df = df.drop(
        df[df.MenuItem.str.contains(r".*for Sand.*", na=False, regex=True)].index
    )
    df = df.drop(
        df[df.MenuItem.str.contains(r".*for Taco.*", na=False, regex=True)].index
    )
    df = df.drop(
        df[df.MenuItem.str.contains(r".*for Cali-Club.*", na=False, regex=True)].index
    )
    df = df.drop(
        df[df.MenuItem.str.contains(r".*for Edge.*", na=False, regex=True)].index
    )
    df = df.drop(
        df[df.MenuItem.str.contains(r".*See Server.*", na=False, regex=True)].index
    )
    df = df.drop(
        df[df.MenuItem.str.contains(r".*Refund.*", na=False, regex=True)].index
    )
    df = df.drop(
        df[df.MenuItem.str.contains(r".*2 Pens.*", na=False, regex=True)].index
    )
    df = df.drop(
        df[df.MenuItem.str.contains(r".*Anniversary.*", na=False, regex=True)].index
    )
    df = df.drop(
        df[df.MenuItem.str.contains(r".*Birthday.*", na=False, regex=True)].index
    )
    df = df.drop(df[df.MenuItem.str.contains(r".*Rare.*", na=False, regex=True)].index)
    df = df.drop(
        df[df.MenuItem.str.contains(r".*Medium.*", na=False, regex=True)].index
    )
    df = df.drop(df[df.MenuItem.str.contains(r".*Well.*", na=False, regex=True)].index)

    return df


def removePreMods(df):
    # Remove toast Pre-Mods from MenuItem strings
    pre_mods = ["Add", "Extra", "Lite", "On Side"]
    post_mods = ["On Side", "Only"]

    # Create regex patterns for pre and post modifications
    pre_pattern = r"^(" + "|".join(pre_mods) + r")\s+"
    post_pattern = r"\s+(" + "|".join(post_mods) + r")$"

    # Apply the regex patterns to the MenuItem column
    df["MenuItem"] = df["MenuItem"].apply(lambda x: re.sub(pre_pattern, "", x))
    df["MenuItem"] = df["MenuItem"].apply(lambda x: re.sub(post_pattern, "", x))

    return df


# used for adding bread basket to menu engineering
def add_new_row(location, menu_item, cost, df):
    new_row = pd.DataFrame(
        {"Location": location, "MenuItem": menu_item, "Cost": cost},
        index=[0],
    )
    return pd.concat([df, new_row], ignore_index=True)


def format_excel(file_path):
    wb = openpyxl.load_workbook(file_path)

    for sheet_name in wb.sheetnames:
        sheet = wb[sheet_name]

        if "currency" not in wb.named_styles:
            currency_style = NamedStyle(name="currency", number_format="#,##0.00")
            for col in ["C", "D", "E", "F", "G", "H", "I"]:
                for cell in sheet[col]:
                    cell.style = currency_style

            for cell in sheet["F"]:
                cell.number_format = "0.0%"

            for col in sheet.columns:
                max_length = 0
                column = col[0].column_letter
                for cell in col:
                    try:
                        if len(str(cell.value)) > max_length:
                            max_length = len(cell.value)
                    except Exception as e:
                        print(repr(e))
                        pass
                adjusted_width = (max_length + 2) * 1.2
                sheet.column_dimensions[column].width = adjusted_width

    wb.save(file_path)


def main(product_mix_csv, menu_analysis_csv, sort_unit):
    product_mix = pd.read_csv(product_mix_csv, skiprows=3, sep=",", thousands=",")
    product_mix.loc[:, "Textbox27"] = product_mix["Textbox27"].str.replace(
        r"CHOPHOUSE\ -\ NOLA", "CHOPHOUSE-NOLA", regex=True
    )
    product_mix.loc[:, "Textbox27"] = product_mix["Textbox27"].str.replace(
        r"CAFÉ", "CAFE", regex=True
    )
    product_mix.loc[:, "Textbox27"] = product_mix["Textbox27"].str.replace(
        r"^(?:.*?( -)){2}", "-", regex=True
    )
    product_mix.loc[:, "TransferDate"] = product_mix["TransferDate"].str.replace(
        r"^(?:.*?( -)){2}", "-", regex=True
    )

    menu_analysis = pd.read_csv(menu_analysis_csv, skiprows=3, sep=",", thousands=",")
    menu_analysis.loc[:, "Location"] = menu_analysis["Location"].str.replace(
        r"CHOPHOUSE\ -\ NOLA", "CHOPHOUSE-NOLA", regex=True
    )
    menu_analysis.loc[:, "Location"] = menu_analysis["Location"].str.replace(
        r"CAFÉ", "CAFE", regex=True
    )
    menu_analysis.loc[:, "Location"] = menu_analysis["Location"].str.replace(
        r"^(?:.*?( -)){2}", "-", regex=True
    )
    menu_analysis.loc[:, "MenuItemName"] = menu_analysis["MenuItemName"].str.replace(
        r"^(?:.*?( -)){2}", "-", regex=True
    )

    menu_analysis["Location"] = menu_analysis["Location"].str.strip()
    df_nonetab = pd.DataFrame()
    store_list = product_mix["Textbox27"].unique()
    # store_list = removedups(store_list)

    # Make dictionary of dataframes for each location
    product_dict = {store: make_dataframe(store, product_mix) for store in store_list}
    price_dict = {store: make_dataframe1(store, menu_analysis) for store in store_list}

    for key in product_dict.keys():
        product_dict[key][["x", "MenuItem"]] = product_dict[key][
            "TransferDate"
        ].str.split(" - ", expand=True)
        # product_dict[key]["Textbox27"].rename("Location", inplace=True)
        product_dict[key].rename(
            columns={"Cost": "Price", "Total": "Sales"}, inplace=True
        )
        product_dict[key].drop(
            columns={
                "Textbox3",
                "x",
                "TransferDate",
                "ToLocationName",
                "dm_Quantity",
                "Textbox17",
                "Textbox20",
            },
            inplace=True,
        )
        df_pmix = product_dict[key].reindex(
            columns=[
                "MenuItem",
                "Qty",
                "Price",
                "Sales",
                "Cat1",
                "Cat2",
                "Cat3",
            ]
        )
        # set MenuItem data type to string
        df_pmix["MenuItem"] = df_pmix["MenuItem"].astype(str)
        # Calculate Bread Basket usage for stores with bread
        stores_w_bread = [
            "NEW YORK PRIME-MYRTLE BEACH",
            "NEW YORK PRIME-BOCA",
            "NEW YORK PRIME-ATLANTA",
            "CHOPHOUSE-NOLA",
            "CHOPHOUSE '47",
            "GULFSTREAM CAFE",
        ]
        entree_count = df_pmix.loc[df_pmix["Cat2"] == "Entree", "Qty"].sum()
        if product_dict[key].iloc[0, 0] in stores_w_bread:
            # add bread basket to df_menu
            new_row = {
                "MenuItem": "Bread Basket per Entree",
                "Qty": entree_count,
                "Price": 0,
                "Sales": 0,
                "Cat1": "Food",
                "Cat2": "No Charge",
                "Cat3": "None",
            }
            df_pmix = pd.concat(
                [df_pmix, pd.DataFrame(new_row, index=[0])], ignore_index=True
            )

        product_dict[key] = df_pmix

    # Menu Price Analysis used for food cost info.
    for key in price_dict.keys():
        price_dict[key].rename(columns={"UnitCost_Loc": "Cost"}, inplace=True)
        try:
            price_dict[key][["X", "MenuItem"]] = price_dict[key][
                "MenuItemName"
            ].str.split(" - ", expand=True)
        except KeyError:
            print(f'{price_dict[key]["MenuItemName"]} "key error"')
        except Exception as e:
            print(repr(e))
        df_pmix = price_dict[key].reindex(columns=["Location", "MenuItem", "Cost"])
        df_pmix["MenuItem"] = df_pmix["MenuItem"].astype(str)

        location = df_pmix.iloc[0, 0]

        # Define a dictionary to map location names to their respective data.
        location_data = {
            "CHOPHOUSE-NOLA": ("Bread Basket per Entree", 0.35),
            "CHOPHOUSE '47": ("Bread Basket per Entree", 0.35),
            "GULFSTREAM CAFE": ("Bread Basket per Entree", 0.92),
            "NEW YORK PRIME-MYRTLE BEACH": ("Bread Basket per Entree", 0.79),
            "NEW YORK PRIME-BOCA": ("Bread Basket per Entree", 0.85),
            "NEW YORK PRIME-ATLANTA": ("Bread Basket per Entree", 0.83),
        }

        # Check if the location exists in the dictionary, then add the new row.
        if location in location_data:
            menu_item, cost = location_data[location]
            df_pmix = add_new_row(location, menu_item, cost, df_pmix)
        price_dict[key] = df_pmix

    directory = "/home/wandored/Projects/menu-engineering/output/"
    for store in store_list:
        df_menu = pd.merge(
            product_dict[store],
            price_dict[store],
            on="MenuItem",
            how="left",
            sort=False,
        )
        df_menu.rename(columns={"Location_x": "Location"}, inplace=True)
        df_pmix = df_menu.reindex(
            columns=[
                "Location",
                "MenuItem",
                "Qty",
                "Price",
                "Sales",
                "Cost",
                "Cat1",
                "Cat2",
                "Cat3",
            ]
        )
        df_menu = removeSpecial(df_pmix)
        df_menu = removePreMods(df_menu)

        cat2_list = df_menu["Cat2"]
        cat2_list = removedups(cat2_list)
        # if "nan" in cat2_list replace it with "None"
        cat2_list = [x if str(x) != "nan" else "None" for x in cat2_list]
        df_menu["cost"] = df_menu["Cost"].fillna(0)
        df_menu["Cost %"] = df_menu.apply(
            lambda row: row.Cost / row.Price if row.Price else 0, axis=1
        )
        df_menu["Margin"] = df_menu.apply(lambda row: row.Price - row.Cost, axis=1)
        df_menu["Total Cost"] = df_menu.apply(lambda row: row.Qty * row.Cost, axis=1)
        df_menu["Profit"] = df_menu.apply(lambda row: row.Qty * row.Margin, axis=1)
        df_pmix = df_menu.reindex(
            columns=[
                "Location",
                "MenuItem",
                "Qty",
                "Price",
                "Cost",
                "Margin",
                "Cost %",
                "Sales",
                "Total Cost",
                "Profit",
                "Cat1",
                "Cat2",
                "Cat3",
            ]
        )
        df_menu = df_pmix
        # select all rows where cat2 is nan
        df_none = df_menu[df_menu["Cat2"].isnull()]
        # Fill NaN values in menu["cat2"] with "None"
        df_menu["Cat2"] = df_menu["Cat2"].fillna("None")
        columns_to_fill_none = ["Cat1", "Cat2", "Cat3"]
        df_none.loc[:, columns_to_fill_none] = (
            df_none.loc[:, columns_to_fill_none].fillna("None").astype(str)
        )
        # # # Fill NaN values in all other columns with 0
        df_none = df_none.fillna(0)

        if not df_none.empty:
            df_nonetab = pd.concat([df_nonetab, df_none])

        # drop nan from cat2_list
        cat2_list = [x for x in cat2_list if str(x) != "nan"]

        with pd.ExcelWriter(
            f"{directory}/{store}.xlsx"
        ) as writer:  # pylint: disable=abstract-class-instantiated
            for cat in cat2_list:
                df = menucatagory(cat, df_menu)
                df = engineer(df)
                df["rating"] = df.apply(rating, axis=1)
                df.sort_values(
                    by=["Profit", "Qty"],
                    inplace=True,
                    ascending=False,
                    ignore_index=True,
                )
                df.drop(columns={"Location", "Cat3", "qty_mn", "mrg_mn"}, inplace=True)
                df.loc["Total"] = pd.Series(
                    df[["Qty", "Sales", "Total Cost", "Profit"]].sum()
                )
                if df.at["Total", "Sales"]:
                    df.at["Total", "Cost %"] = (
                        df.at["Total", "Total Cost"] / df.at["Total", "Sales"]
                    )
                else:
                    df.at["Total", "Cost %"] = 1
                df.to_excel(writer, sheet_name=cat, index=False)

    # Format excel files
    # excel_files = [file for file in os.listdir(directory) if file.endswith(".xlsx")]
    # for file in excel_files:
    #     file_path = os.path.join(directory, file)
    #     format_excel(file_path)

    if df_nonetab.empty:
        print("No items to add")
    df_nonetab = df_nonetab.groupby(["MenuItem"]).sum(numeric_only=True)
    df_nonetab.sort_values(by=sort_unit, inplace=True, ascending=False)
    print(df_nonetab.head(25))
    print()
    print("--------------------------------------------------------------------")
    print(df_nonetab.info())
    print(df_nonetab.describe())


if __name__ == "__main__":
    # creat and argument parser object
    parser = argparse.ArgumentParser()
    # check for user provide argument
    parser.add_argument(
        "-q", "--quantity", help="Sort results by quantity sold", action="store_true"
    )
    parser.add_argument(
        "-s", "--sales", help="Sort results by sales", action="store_true"
    )
    args = parser.parse_args()

    # must have a sort unit
    if args.sales:
        sort_unit = "Sales"
    elif args.quantity:
        sort_unit = "Qty"
    else:
        print("No sort unit provided, sort will be by Sales Total")
        sort_unit = "Sales"

    product_mix = "./downloads/Product Mix.csv"
    menu_price_analysis = "./downloads/Menu Price Analysis.csv"
    main(product_mix, menu_price_analysis, sort_unit)

    os.system(
        "cp /home/wandored/Projects/menu-engineering/output/*.xlsx /home/wandored/Sync/ReportData/"
    )
