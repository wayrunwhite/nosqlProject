from cassandra.cluster import Cluster  # type: ignore
from uuid import uuid4
from datetime import datetime

# connexion au cluster Cassandra
def connect_cassandra():
    # Connecter au cluster (par défaut sur localhost:9042)
    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect('gameforge')  # utiliser le keyspace gameforge
    return session

# fonction CREATE
def create_action(session, player_id, type_action, xp, timestamp):
    query = """
    INSERT INTO statistiques_joueur (player_id, type_action, xp, timestamp)
    VALUES (%s, %s, %s, %s)
    """
    session.execute(query, (player_id, type_action, xp, timestamp))
    print(f"Action {type_action} ajoutée pour le joueur {player_id}.")

    # maj du classement
    periode = timestamp.strftime('%Y-%m')  # Période mensuelle
    update_classement(session, player_id, type_action, xp, periode)

# fonction READ
def read_actions(session, player_id):
    query = "SELECT * FROM statistiques_joueur WHERE player_id = %s"
    rows = session.execute(query, (player_id,))
    for row in rows:
        print(f"Player: {row.player_id}, Action: {row.type_action}, XP: {row.xp}, Timestamp: {row.timestamp}")

# Fonction UPDATE classement
def update_classement(session, player_id, type_action, xp, periode):
    # mettre a jour total_xp dans classements_joueurs en utilisant le compteur
    query_update = """
    UPDATE classements_joueurs
    SET total_xp = total_xp + %s
    WHERE type_action = %s AND periode = %s AND player_id = %s
    """
    session.execute(query_update, (xp, type_action, periode, player_id))

# fonction pour obtenir le top des joueurs
def get_top_players(session, type_action, periode, limit=10):
    query = """
    SELECT player_id, total_xp FROM classements_joueurs
    WHERE type_action = %s AND periode = %s
    """
    rows = session.execute(query, (type_action, periode))
    # convertir les résultats en liste
    rows_list = list(rows)
    # trier les joueurs par total_xp décroissant
    sorted_rows = sorted(rows_list, key=lambda x: x.total_xp, reverse=True)
    # afficher les top joueurs
    for row in sorted_rows[:limit]:
        print(f"Player: {row.player_id}, Total XP: {row.total_xp}")

# fonction UPDATE XP
def update_xp(session, player_id, timestamp, new_xp):
    query = """
    UPDATE statistiques_joueur
    SET xp = %s
    WHERE player_id = %s AND timestamp = %s
    """
    session.execute(query, (new_xp, player_id, timestamp))
    print(f"XP mis à jour pour le joueur {player_id} à {new_xp} points.")

# Fonction DELETE
def delete_action(session, player_id, timestamp):
    query = """
    DELETE FROM statistiques_joueur
    WHERE player_id = %s AND timestamp = %s
    """
    session.execute(query, (player_id, timestamp))
    print(f"Action supprimée pour le joueur {player_id} à la date {timestamp}.")

# Fonction principale pour tester les CRUD et les classements
def main():
    # pour se connecter à Cassandra
    session = connect_cassandra()

    # creation d'une action pour un joueur
    player_id = uuid4()  # Création d'un nouvel ID de joueur
    timestamp = datetime.utcnow()  # Date actuelle
    create_action(session, player_id, 'attaque', 150, timestamp)

    # Lecture des actions pour ce joueur
    read_actions(session, player_id)

    # mise à jour de l'XP d'une action
    update_xp(session, player_id, timestamp, 200)

    # Suppression de l'action
    delete_action(session, player_id, timestamp)

    # Creation d'actions pour plusieurs joueurs
    players = [uuid4() for _ in range(5)]  # Création de 5 joueurs
    timestamp = datetime.utcnow()  # Date actuelle

    # Simule des actions
    for idx, pid in enumerate(players):
        xp = 100 + idx * 50
        create_action(session, pid, 'attaque', xp, timestamp)

    # Récupére le classement
    periode = timestamp.strftime('%Y-%m')
    print(f"\nClassement des joueurs pour la période {periode}:")
    get_top_players(session, 'attaque', periode)

    # Fermeture de la session
    session.shutdown()
    print("\nOpérations terminées.")

# Exécute la fonction principale
if __name__ == "__main__":
    main()
