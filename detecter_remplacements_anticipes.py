import pandas as pd
from datetime import datetime
import os

def load_data(file_path):
    """Charge le fichier CSV nettoyé et filtré."""
    try:
        df = pd.read_csv(file_path, encoding='utf-8')
        # S'assurer que date_mod est au format datetime
        df['date_mod'] = pd.to_datetime(df['date_mod'], errors='coerce')
        return df
    except Exception as e:
        print(f"Erreur lors du chargement de {file_path} : {e}")
        return None

def load_users(file_path):
    """Charge les utilisateurs depuis utilisateurs.csv et retourne un dictionnaire {utilisateur_id: nom}."""
    try:
        users_df = pd.read_csv(file_path, encoding='utf-8')
        # S'assurer que utilisateur_id est en string pour matcher avec users_id
        users_df['utilisateur_id'] = users_df['utilisateur_id'].astype(str)
        # Créer un mapping
        user_map = dict(zip(users_df['utilisateur_id'], users_df['nom_utilisateur']))  # ou 'nom' selon ton CSV
        return user_map
    except Exception as e:
        print(f"Erreur lors du chargement de {file_path} : {e}")
        return None

def detecter_remplacements_anticipes(df, user_map):
    """
    Détecte les remplacements anticipés (< 2 ans entre deux attributions pour un même utilisateur).
    Retourne deux DataFrames :
    1. remplacements_df : tous les cas de remplacement anticipé (avec détails, incluant noms des téléphones)
    2. utilisateurs_df : résumé par utilisateur (nombre de remplacements anticipés)
    """
    # Filtrer uniquement les smartphones attribués (states_id == 2 et users_id > 0)
    df = df[(df['states_id'] == 2) & (df['users_id'] > 0)].copy()
    
    # Convertir users_id en string pour matcher avec les mappings
    df['users_id'] = df['users_id'].astype(str)
    
    # Trier par utilisateur puis par date
    df = df.sort_values(['users_id', 'date_mod']).reset_index(drop=True)
    
    remplacements = []
    utilisateurs = {}

    # Parcourir chaque utilisateur
    for user_id, group in df.groupby('users_id'):
        if len(group) < 2:
            continue  # Pas de remplacement possible avec un seul téléphone
        
        dates = group['date_mod'].tolist()
        rows = group.to_dict('records')
        
        for i in range(1, len(dates)):
            date_actuelle = dates[i]
            date_precedente = dates[i-1]
            
            if pd.isna(date_actuelle) or pd.isna(date_precedente):
                continue
            
            # Calculer la différence en jours, puis convertir en années
            diff_jours = (date_actuelle - date_precedente).days
            diff_annees = diff_jours / 365.25  # Compte les années bissextiles
            
            if diff_annees < 2.0:
                # Récupérer le nom de l'utilisateur depuis le mapping
                nom_utilisateur = user_map.get(user_id, 'Inconnu')
                # Récupérer les noms des téléphones depuis la colonne 'name'
                nom_tele_precedent = rows[i-1].get('name', 'Inconnu')
                nom_tele_actuel = rows[i].get('name', 'Inconnu')

                # C'est un remplacement anticipé
                remplacement = {
                    'users_id': user_id,
                    'nom_utilisateur': nom_utilisateur,
                    'nom_tele_precedent': nom_tele_precedent,
                    'date_precedente': date_precedente.strftime('%Y-%m-%d'),
                    'nom_tele_actuel': nom_tele_actuel,
                    'date_actuelle': date_actuelle.strftime('%Y-%m-%d'),
                    'intervalle_jours': int(diff_jours),
                    'intervalle_annees': round(diff_annees, 2)
                }
                remplacements.append(remplacement)
                
                # Compter par utilisateur
                if user_id not in utilisateurs:
                    utilisateurs[user_id] = {
                        'users_id': user_id,
                        'nom_utilisateur': nom_utilisateur,
                        'nb_remplacements_anticipes': 0
                    }
                utilisateurs[user_id]['nb_remplacements_anticipes'] += 1

    # Convertir en DataFrames
    remplacements_df = pd.DataFrame(remplacements)
    utilisateurs_df = pd.DataFrame(list(utilisateurs.values())).sort_values('nb_remplacements_anticipes', ascending=False)

    return remplacements_df, utilisateurs_df

def sauvegarder_resultats(remplacements_df, utilisateurs_df):
    """Sauvegarde les résultats dans des fichiers CSV."""
    try:
        remplacements_df.to_csv('remplacements_anticipes.csv', index=False, encoding='utf-8')
        print(f"{len(remplacements_df)} remplacements anticipés sauvegardés dans 'remplacements_anticipes.csv'")
    except Exception as e:
        print(f"Erreur lors de la sauvegarde des remplacements : {e}")

    try:
        utilisateurs_df.to_csv('utilisateurs_multi_remplacements.csv', index=False, encoding='utf-8')
        print(f"{len(utilisateurs_df)} utilisateurs concernés sauvegardés dans 'utilisateurs_multi_remplacements.csv'")
    except Exception as e:
        print(f"Erreur lors de la sauvegarde des utilisateurs : {e}")

def main():
    input_file = "cleaned_telephones_filtered.csv"
    utilisateurs_file = "exports/utilisateurs.csv"
    
    # Vérifier l'existence des fichiers
    if not os.path.exists(input_file):
        print(f"Le fichier {input_file} est introuvable. Assurez-vous que le nettoyage et le filtrage sont terminés.")
        return

    if not os.path.exists(utilisateurs_file):
        print(f"Le fichier {utilisateurs_file} est introuvable. Impossible de récupérer les noms des utilisateurs.")
        return

    print("Chargement des données...")
    df = load_data(input_file)
    if df is None:
        return

    print("Chargement des noms d'utilisateurs...")
    user_map = load_users(utilisateurs_file)
    if user_map is None:
        return

    print("Détection des remplacements anticipés...")
    remplacements_df, utilisateurs_df = detecter_remplacements_anticipes(df, user_map)

    if len(remplacements_df) == 0:
        print("Aucun remplacement anticipé détecté !")
    else:
        print(f"{len(remplacements_df)} remplacements anticipés trouvés.")
        print(f"{len(utilisateurs_df)} utilisateurs concernés.")

    sauvegarder_resultats(remplacements_df, utilisateurs_df)

    # Afficher un résumé
    if not utilisateurs_df.empty:
        print("Top 5 des utilisateurs avec le plus de remplacements anticipés :")
        print(utilisateurs_df.head().to_string(index=False))

if __name__ == "__main__":
    main()