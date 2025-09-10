import pandas as pd
from sqlalchemy import create_engine
import os
from urllib.parse import quote_plus

connection = {
    'host': 'localhost',
    'port': 3306,
    'user': 'root',
    'password': 'Khadija@2005',
    'database': 'telephones_db'
}

output = "exports"

def get_db():
    password = quote_plus(connection['password'])
    connection_string = f"mysql+mysqlconnector://{connection['user']}:{password}@{connection['host']}:{connection['port']}/{connection['database']}"
    engine = create_engine(connection_string)
    return engine

def get_tables(engine):
    query = "SHOW TABLES"
    df = pd.read_sql(query, engine)
    return df.iloc[:, 0].tolist()

def export_table_to_csv(engine, table_name, output_dir):
    try:
        df = pd.read_sql(f"SELECT * FROM {table_name}", engine)
        os.makedirs(output_dir, exist_ok=True)
        df.to_csv(f"{output_dir}/{table_name}.csv", index=False, encoding='utf-8')
        print(f"Exporté : {table_name}.csv")
        return f"{table_name}.csv"
    except Exception as e:
        error_msg = f"Erreur lors de l'exportation de {table_name} : {str(e)}"
        print(error_msg)
        return error_msg

def export_mysql_to_csv():
    """Fonction principale pour exporter toutes les tables MySQL en CSV"""
    try:
        print("[INFO] Démarrage de l'exportation MySQL vers CSV...")
        
        engine = get_db()
        print("Moteur de connexion créé")
        
        tables = get_tables(engine)
        print("Connexion testée avec succès")
        print(f"Tables trouvées : {tables}")
        
        exported_files = []
        for table in tables:
            result = export_table_to_csv(engine, table, output)
            if result.endswith('.csv'):
                exported_files.append(result)
        
        engine.dispose()
        
        summary = f"""
Exportation terminée avec succès !
Tables exportées : {len(exported_files)}
Fichiers créés : {', '.join(exported_files)}
Dossier de sortie : {output}/
"""
        print(summary)
        return summary
        
    except Exception as e:
        error_msg = f"Erreur générale : {str(e)}"
        print(error_msg)
        return error_msg

if __name__ == "__main__":
    result = export_mysql_to_csv()
    print(f"Résultat final : {result}")