
import pandas as pd
import pyodbc


SERVER = 'KEVINE\\SQLEXPRESS'
DATABASE = 'db_ORAU'
#USERNAME = 'your_username'
#PASSWORD = 'your_password'
DRIVER = '{ODBC Driver 17 for SQL Server}'


EXCEL_FILE_URL = 'https://github.com/eunice196/DONNEES_EXCEL/blob/main/DONNEE_DE_DECEMBRE.xlsx'

def connect_to_db():
    cnxn_str = (
        f'DRIVER={DRIVER};'
        f'SERVER={SERVER};'
        f'DATABASE={DATABASE};'
        #f'UID={USERNAME};'
        #f'PWD={PASSWORD}'
        f'Trusted_Connection=yes;'
    )
    try:
        cnxn = pyodbc.connect(cnxn_str)
        print("Connexion à la base de données Azure SQL réussie.")
        return cnxn
    except pyodbc.Error as ex:
        sqlstate = ex.args[0]
        print(f"Erreur de connexion à la base de données: {sqlstate}")
        raise

def insert_stock_data(df_stock, cnxn):
    cursor = cnxn.cursor()
    insert_sql = """
    INSERT INTO StockBrut (MOIS_NUM, MOIS, ANNEE, PRES, REGION, DISTRICT, SITE, CODE_PRODUIT, PRODUIT, Conditionnement, SDU, CMM)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    for index, row in df_stock.iterrows():
        try:
            cursor.execute(insert_sql,
                           row['MOIS_NUM'], row['MOIS'], row['ANNEE'], row['PRES'], row['REGION'],
                           row['DISTRICT'], row['SITE'], row['CODE_PRODUIT'], row['PRODUIT'],
                           row['Conditionnement'], row['SDU'], row['CMM'])
        except pyodbc.Error as ex:
            sqlstate = ex.args[0]
            print(f"Erreur lors de l'insertion dans StockBrut pour la ligne {index}: {sqlstate} - {row.to_dict()}")
            cnxn.rollback() # Annuler la transaction en cas d'erreur
            raise
    cnxn.commit()
    print(f"{len(df_stock)} lignes insérées dans StockBrut.")

def insert_distribution_data(df_dist, cnxn):
    cursor = cnxn.cursor()
    insert_sql = """
    INSERT INTO Distribution (MOIS_NUM, MOIS, ANNEE, CODE_PRODUIT, PRES_RECEVEUR, PRES_DONNEUR, SITE_DONNEUR, SITE_RECEVEUR, REGION_DONNEUR, DISTRICT_DONNEUR, REGION_RECEVEUR, DISTRICT_RECEVEUR, PRODUIT, Conditionnement, Statut, QTE)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    for index, row in df_dist.iterrows():
        try:
            # Assurez-vous que 'Statut' est bien géré, par défaut 'Non validé'
            statut = row.get('Statut', 'Non validé') # Utiliser .get() pour gérer les colonnes manquantes
            cursor.execute(insert_sql,
                           row['MOIS_NUM'], row['MOIS'], row['ANNEE'], row['CODE_PRODUIT'],
                           row['PRES_RECEVEUR'], row['PRES_DONNEUR'], row['SITE_DONNEUR'],
                           row['SITE_RECEVEUR'], row['REGION_DONNEUR'], row['DISTRICT_DONNEUR'],
                           row['REGION_RECEVEUR'], row['DISTRICT_RECEVEUR'], row['PRODUIT'],
                           row['Conditionnement'], statut, row['QTE'])
        except pyodbc.Error as ex:
            sqlstate = ex.args[0]
            print(f"Erreur lors de l'insertion dans Distribution pour la ligne {index}: {sqlstate} - {row.to_dict()}")
            cnxn.rollback() # Annuler la transaction en cas d'erreur
            raise
    cnxn.commit()
    print(f" {len(df_dist)} lignes insérées dans Distribution.")

def main():
    try:
        # Lire le fichier Excel
        df_stock = pd.read_excel(EXCEL_FILE_URL, sheet_name='ETAT DE STOCK')
        df_dist = pd.read_excel(EXCEL_FILE_URL, sheet_name='DISTRIBUTION')

        # Nettoyage des colonnes 'Unnamed' si elles existent dans DISTRIBUTION
        df_dist = df_dist.loc[:, ~df_dist.columns.str.contains('^Unnamed')]
        # Renommer la colonne ' ' en 'Statut' si elle existe et 'Statut' n'existe pas
        if ' ' in df_dist.columns and 'Statut' not in df_dist.columns:
            df_dist.rename(columns={' ': 'Statut'}, inplace=True)
        # Remplir les valeurs NaN dans 'Statut' avec 'Non validé' si la colonne existe
        if 'Statut' in df_dist.columns:
            df_dist['Statut'].fillna('Non validé', inplace=True)
        else:
            # Si la colonne 'Statut' n'existe pas du tout, l'ajouter avec la valeur par défaut
            df_dist['Statut'] = 'Non validé'

        # Connexion à la base de données
        cnxn = connect_to_db()

        # Insertion des données
        print("Début de l'insertion des données de StockBrut...")
        insert_stock_data(df_stock, cnxn)
        print("Début de l'insertion des données de Distribution...")
        insert_distribution_data(df_dist, cnxn)

    except Exception as e:
        print(f"Une erreur générale est survenue: {e}")
    finally:
        if 'cnxn' in locals() and cnxn:
            cnxn.close()
            print("Connexion à la base de données fermée.")

if __name__ == "__main__":
    main()
