import subprocess
import sys
import os

def run_script(script_name):
    """Exécute un script Python et vérifie s'il s'est terminé avec succès.
    Utilise l'encodage cp1252 pour éviter les erreurs sur Windows (caractères accentués).
    """
    print(f"Exécution de {script_name}...")
    try:
        result = subprocess.run(
            [sys.executable, script_name],
            capture_output=True,
            text=True,
            encoding='cp1252',
            errors='replace'
        )
    except Exception as e:
        print(f"Erreur critique lors de l'exécution de {script_name} : {e}")
        sys.exit(1)

    if result.returncode == 0:
        print(f"{script_name} exécuté avec succès.")
        output = result.stdout.strip()
        if output:
            print("Sortie :")
            print(output)
    else:
        print(f"Erreur lors de l'exécution de {script_name} :")
        print(result.stderr)
        sys.exit(1)

def check_file_exists(filepath):
    """Vérifie si un fichier existe, sinon quitte avec erreur."""
    if not os.path.exists(filepath):
        print(f"Fichier attendu introuvable : {filepath}")
        sys.exit(1)
    else:
        print(f"Fichier trouvé : {filepath}")

def main():
    print("[INFO] Lancement du workflow complet :")
    print("   Transformer -> Cleaner -> Filter -> Détecter Remplacements Anticipés")
    print("   (Projet Power BI - Gestion des téléphones - Résidences Dar Saada)")

    # Étape 1 : Transformer (Export MySQL -> CSV)
    print("ÉTAPE 1 : Export des données depuis MySQL")
    run_script("transformer.py")
    check_file_exists("exports/telephones.csv")
    check_file_exists("exports/utilisateurs.csv")
    check_file_exists("exports/modeles_telephones.csv")

    # Étape 2 : Cleaner (Nettoyage + Filtrage)
    print("ÉTAPE 2 : Nettoyage et filtrage des données")
    run_script("cleaner.py")
    check_file_exists("cleaned_telephones.csv")
    check_file_exists("isolated_telephones.csv")
    check_file_exists("cleaned_telephones_filtered.csv")

    # Étape 3 : Détection des remplacements anticipés
    print("ÉTAPE 3 : Détection des remplacements anticipés (< 2 ans)")
    run_script("detecter_remplacements_anticipes.py")
    check_file_exists("remplacements_anticipes.csv")
    check_file_exists("utilisateurs_multi_remplacements.csv")

    # Résumé final
    print("WORKFLOW TERMINÉ AVEC SUCCÈS !")

    # Compter les lignes pour afficher un résumé quantitatif
    def count_lines(filename):
        try:
            with open(filename, 'r', encoding='utf-8') as f:
                return sum(1 for line in f) - 1  # -1 pour l'en-tête
        except:
            return 0

    total_remplacements = count_lines("remplacements_anticipes.csv")
    total_utilisateurs = count_lines("utilisateurs_multi_remplacements.csv")

    print("RÉSUMÉ DES RÉSULTATS :")
    print(f"   - Remplacements anticipés détectés : {total_remplacements}")
    print(f"   - Utilisateurs concernés : {total_utilisateurs}")

    print("Fichiers générés :")

    print("Phase 1 - Export brut :")
    print("   - exports/telephones.csv")
    print("   - exports/utilisateurs.csv")
    print("   - exports/modeles_telephones.csv")

    print("Phase 2 - Nettoyage & Filtrage :")
    print("   - cleaned_telephones.csv (tous les téléphones nettoyés)")
    print("   - isolated_telephones.csv (users_id=0 & states_id=2)")
    print("   - cleaned_telephones_filtered.csv (seulement utilisateurs valides)")

    print("Phase 3 - Analyse métier (conforme Page 3 du rapport) :")
    print("   - remplacements_anticipes.csv - Détail des attributions < 2 ans")
    print("   - utilisateurs_multi_remplacements.csv - Top utilisateurs avec plusieurs remplacements")

    print("Prochaine étape :")
    print("   Importer ces fichiers dans Power BI pour recréer le rapport décrit dans le stage :")
    print("   - Page 1 : Stock & Attribution")
    print("   - Page 2 : Top utilisateurs & modèles")
    print("   - Page 3 : Remplacements anticipés & multi-attributions")

if __name__ == "__main__":
    main()