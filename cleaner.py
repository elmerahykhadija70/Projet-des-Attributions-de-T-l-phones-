import csv
import os
import pandas as pd
import re
from datetime import datetime

# Fonctions de nettoyage

def extract_date_from_string(text):
    """
    Extrait la première date d'une chaîne en utilisant des motifs courants.
    Motifs : JJ/MM/AAAA, AAAA-MM-JJ, ou avec heure.
    Retourne la date au format AAAA-MM-JJ ou None si non trouvée.
    """
    if not isinstance(text, str):
        return None

    # Regex pour JJ/MM/AAAA (optionnellement avec heure)
    pattern1 = r'(\d{2}/\d{2}/\d{4})(?:\s+\d{2}:\d{2}:\d{2})?'
    # Regex pour AAAA-MM-JJ (optionnellement avec heure)
    pattern2 = r'(\d{4}-\d{2}-\d{2})(?:\s+\d{2}:\d{2}:\d{2})?'

    match1 = re.search(pattern1, text)
    match2 = re.search(pattern2, text)

    if match1:
        try:
            dt = datetime.strptime(match1.group(1), '%d/%m/%Y')
            return dt.strftime('%Y-%m-%d')
        except ValueError:
            return None
    elif match2:
        try:
            dt = datetime.strptime(match2.group(1), '%Y-%m-%d')
            return dt.strftime('%Y-%m-%d')
        except ValueError:
            return None
    return None

def clean_dataset(file_path, output_cleaned_path='cleaned_telephones.csv', output_isolated_path='isolated_telephones.csv'):
    """
    Nettoie le jeu de données :
    - Supprime les doublons
    - Remplit date_mod dans l'ordre :
        1. date_creation
        2. comment
        3. contact
        4. (dernier recours) modeles_telephones.csv via phonemodels_id → modele_id → date_modification
    - Isole les lignes users_id == 0 et states_id == 2
    - Sauvegarde les résultats
    """
    # Charger le jeu de données principal
    try:
        df = pd.read_csv(file_path, encoding='utf-8')
    except FileNotFoundError:
        print(f"[ERREUR] Fichier {file_path} introuvable.")
        return None, None
    except Exception as e:
        print(f"[ERREUR] Erreur de chargement : {e}")
        return None, None

    # Supprimer les doublons
    initial_rows = len(df)
    df = df.drop_duplicates()
    print(f"[SUPPRESSION] Supprimé {initial_rows - len(df)} doublons.")

    # Identifier les lignes avec date_mod manquant ou vide
    mask = df['date_mod'].isna() | (df['date_mod'] == '')
    print(f"[DATE] {mask.sum()} valeurs de 'date_mod' à remplir...")

    # Étape 1 : Remplir à partir de date_creation
    filled_count = 0
    for idx in df[mask].index:
        extracted = extract_date_from_string(df.at[idx, 'date_creation'])
        if extracted:
            df.at[idx, 'date_mod'] = extracted + ' 00:00:00'
            filled_count += 1
    print(f"  {filled_count} lignes remplies via 'date_creation'.")
    mask = df['date_mod'].isna() | (df['date_mod'] == '')

    # Étape 2 : Remplir à partir de comment
    filled_count = 0
    for idx in df[mask].index:
        extracted = extract_date_from_string(df.at[idx, 'comment'])
        if extracted:
            df.at[idx, 'date_mod'] = extracted + ' 00:00:00'
            filled_count += 1
    print(f"  {filled_count} lignes remplies via 'comment'.")
    mask = df['date_mod'].isna() | (df['date_mod'] == '')

    # Étape 3 : Remplir à partir de contact
    filled_count = 0
    for idx in df[mask].index:
        extracted = extract_date_from_string(df.at[idx, 'contact'])
        if extracted:
            df.at[idx, 'date_mod'] = extracted + ' 00:00:00'
            filled_count += 1
    print(f"  {filled_count} lignes remplies via 'contact'.")
    mask = df['date_mod'].isna() | (df['date_mod'] == '')

    # Étape 4 (dernier recours) : Remplir à partir de modeles_telephones.csv
    try:
        modeles_df = pd.read_csv('exports/modeles_telephones.csv', encoding='utf-8')
        if 'modele_id' in modeles_df.columns and 'date_modification' in modeles_df.columns:
            # Préparer le mapping
            modeles_df['modele_id'] = pd.to_numeric(modeles_df['modele_id'], errors='coerce').astype('Int64')
            modeles_dict = modeles_df.set_index('modele_id')['date_modification'].to_dict()

            # S'assurer que phonemodels_id est du bon type
            if 'phonemodels_id' in df.columns:
                df['phonemodels_id'] = pd.to_numeric(df['phonemodels_id'], errors='coerce').astype('Int64')

                # Remplir les restants via mapping
                before_fill = mask.sum()
                df.loc[mask, 'date_mod'] = df.loc[mask, 'phonemodels_id'].map(modeles_dict)
                after_fill = (df['date_mod'].isna() | (df['date_mod'] == '')).sum()
                filled_count = before_fill - after_fill
                print(f"  {filled_count} lignes remplies via 'modeles_telephones.csv' (dernier recours).")
        else:
            print("[ERREUR] Colonnes 'modele_id' ou 'date_modification' manquantes dans modeles_telephones.csv")
    except FileNotFoundError:
        print("[ALERTE] Fichier exports/modeles_telephones.csv non trouvé — étape ignorée.")
    except Exception as e:
        print(f"[ERREUR] Erreur lors du chargement de modeles_telephones.csv : {e}")

    # Isoler les lignes avec users_id == 0 et states_id == 2
    isolated_df = df[(df['users_id'] == 0) & (df['states_id'] == 2)]
    print(f"[ISOLE] Isolé {len(isolated_df)} lignes avec users_id == 0 et states_id == 2.")

    # Sauvegarder
    try:
        df.to_csv(output_cleaned_path, index=False, encoding='utf-8')
        print(f"[OK] Jeu de données nettoyé sauvegardé : {output_cleaned_path}")
    except Exception as e:
        print(f"[ERREUR] Erreur lors de la sauvegarde cleaned : {e}")

    try:
        isolated_df.to_csv(output_isolated_path, index=False, encoding='utf-8')
        print(f"[OK] Jeu de données isolé sauvegardé : {output_isolated_path}")
    except Exception as e:
        print(f"[ERREUR] Erreur lors de la sauvegarde isolated : {e}")

    return df, isolated_df

# Fonctions de filtrage

def load_users(file_path):
    """Charge les utilisateur_id depuis le fichier utilisateurs.csv."""
    user_ids = set()
    try:
        with open(file_path, mode='r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                user_ids.add(row['utilisateur_id'])
    except FileNotFoundError:
        print(f"[ERREUR] Le fichier {file_path} n'a pas été trouvé.")
        return None
    except Exception as e:
        print(f"[ERREUR] Erreur lors de la lecture de {file_path} : {e}")
        return None
    return user_ids

def filter_telephones(input_telephones_path, output_telephones_path, user_ids):
    """Supprime les lignes de cleaned_telephones.csv où users_id n'est pas dans user_ids."""
    try:
        with open(input_telephones_path, mode='r', encoding='utf-8') as infile:
            reader = csv.DictReader(infile)
            fieldnames = reader.fieldnames
            filtered_rows = []

            for row in reader:
                # Vérifie si users_id est dans user_ids (et non vide)
                user_id = row.get('users_id', '').strip()
                if user_id and user_id in user_ids:
                    filtered_rows.append(row)

        # Écrit les lignes filtrées dans un nouveau fichier
        with open(output_telephones_path, mode='w', encoding='utf-8', newline='') as outfile:
            writer = csv.DictWriter(outfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(filtered_rows)

        print(f"[OK] Fichier filtré créé avec succès : {output_telephones_path}")
        print(f"[INFO] Nombre de lignes conservées : {len(filtered_rows)}")

    except FileNotFoundError:
        print(f"[ERREUR] Le fichier {input_telephones_path} n'a pas été trouvé.")
    except Exception as e:
        print(f"[ERREUR] Erreur lors du traitement de {input_telephones_path} : {e}")

# Fonction principale

def main():
    # Chemins des fichiers
    input_telephones_path = "exports/telephones.csv"
    cleaned_telephones_path = "cleaned_telephones.csv"
    filtered_telephones_path = "cleaned_telephones_filtered.csv"
    utilisateurs_path = "exports/utilisateurs.csv"

    # Étape 1 : Nettoyage du jeu de données
    print("ETAPE 1 : NETTOYAGE DU JEU DE DONNEES")
    cleaned_df, isolated_df = clean_dataset(input_telephones_path, cleaned_telephones_path, 'isolated_telephones.csv')
    if cleaned_df is None:
        print("[ERREUR] Échec du nettoyage. Arrêt du script.")
        return

    # Étape 2 : Vérification des fichiers nécessaires
    print("ETAPE 2 : FILTRAGE PAR UTILISATEURS VALIDES")
    if not os.path.exists(utilisateurs_path):
        print(f"[ERREUR] Le fichier {utilisateurs_path} n'existe pas.")
        return

    if not os.path.exists(cleaned_telephones_path):
        print(f"[ERREUR] Le fichier {cleaned_telephones_path} n'existe pas (nettoyage échoué ?).")
        return

    # Étape 3 : Charger les utilisateur_id valides
    user_ids = load_users(utilisateurs_path)
    if user_ids is None:
        print("[ERREUR] Impossible de charger les utilisateur_id.")
        return

    # Étape 4 : Filtrer cleaned_telephones.csv
    filter_telephones(cleaned_telephones_path, filtered_telephones_path, user_ids)

    print("TRAITEMENT TERMINE :")
    print(f"   - Fichier nettoyé : {cleaned_telephones_path}")
    print(f"   - Fichier filtré : {filtered_telephones_path}")
    print(f"   - Fichier isolé : isolated_telephones.csv")

if __name__ == "__main__":
    main()