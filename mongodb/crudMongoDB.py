from pymongo import MongoClient
from bson.objectid import ObjectId


client = MongoClient("mongodb://localhost:27017/")
db = client["GameForge"]  
players_collection = db["Players"]  
skills_collection = db["Skills"]  
items_collection = db["Items"] 


def get_all_players():
    players = players_collection.find()  
    print("Players:")
    for player in players:
        print(player)


def get_all_skills():
    skills = skills_collection.find()  
    print("\nSkills:")
    for skill in skills:
        print(skill)


def get_all_items():
    items = items_collection.find() 
    print("\nItems:")
    for item in items:
        print(item)


def CreatePlayer(username, player_class):
    if player_class not in ['Warrior', 'Mage', 'Archer', 'Thief']:
        print("Error: Invalid class. Choose from 'Warrior', 'Mage', 'Archer', or 'Thief'.")
        return

    new_player = {
        "player_id": players_collection.count_documents({}) + 1,  
        "username": username,
        "class": player_class,
        "level": 0,
        "last_login": None,
        "skills": [],
        "inventory": []
    }

    try:
        players_collection.insert_one(new_player)
        print(f"Player '{username}' successfully created.")
    except Exception as e:
        print(f"Error creating player: {str(e)}")



def ReadPlayer(player_id):
    try:
        player = players_collection.find_one({"_id": ObjectId(player_id)})  # Recherche du joueur par _id
        if player:
            print("Player found:")
            print(player)
        else:
            print(f"Error: Player with ID {player_id} not found.")
    except Exception as e:
        print(f"Error: {str(e)}")


def UpdatePlayer(player_id, level=None, last_login=None, skill=None, item=None):
    player = players_collection.find_one({"_id": ObjectId(player_id)})
    if not player:
        print(f"Player {player_id} not found.")
        return

    updates = {}
    
    if level is not None:
        updates["level"] = level
        print(f"Level updated to {level} for player {player['username']}.")

    if last_login is not None:
        updates["last_login"] = last_login
        print(f"Last login updated to {last_login} for player {player['username']}.")

    if skill is not None:
        skill_id = ObjectId(skill)
        if not any(s.get("skill_ID") == skill_id for s in player.get("skills", [])):
            players_collection.update_one(
                {"_id": ObjectId(player_id)},
                {"$push": {"skills": {"skill_ID": skill_id}}}
            )
            print(f"Skill {skill} added for player {player['username']}.")
        else:
            print(f"Skill {skill} already exists for player {player['username']}.")

    if item is not None:
        item_id = ObjectId(item)
        inventory_item = next((it for it in player.get("inventory", []) if it.get("item_ID") == item_id), None)
        if inventory_item:
            players_collection.update_one(
                {"_id": ObjectId(player_id), "inventory.item_ID": item_id},
                {"$inc": {"inventory.$.quantity": 1}}
            )
            print(f"Item {item} quantity incremented for player {player['username']}.")
        else:
            players_collection.update_one(
                {"_id": ObjectId(player_id)},
                {"$push": {"inventory": {"item_ID": item_id, "quantity": 1}}}
            )
            print(f"Item {item} added for player {player['username']}.")

    if updates:
        players_collection.update_one({"_id": ObjectId(player_id)}, {"$set": updates})
        print("Updated player:", player)

def RemoveItem(player_id, item_id):
    player = players_collection.find_one({"_id": ObjectId(player_id)})
    if not player:
        print(f"Player {player_id} not found.")
        return

    item_id = ObjectId(item_id)
    players_collection.update_one(
        {"_id": ObjectId(player_id)},
        {"$pull": {"inventory": {"item_ID": item_id}}}
    )
    print(f"Item {item_id} removed from player {player['username']}'s inventory.")


    

def RemoveSkill(player_id, skill_id):
    player = players_collection.find_one({"_id": ObjectId(player_id)})
    if not player:
        print(f"Player {player_id} not found.")
        return

    skill_id = ObjectId(skill_id)
    players_collection.update_one(
        {"_id": ObjectId(player_id)},
        {"$pull": {"skills": {"skill_ID": skill_id}}}
    )
    print(f"Skill {skill_id} removed from player {player['username']}.")


    

def DeletePlayer(player_id):
    try:
        player_id = ObjectId(player_id)
    except Exception as e:
        print(f"Error: Invalid player_id format. {str(e)}")
        return

    result = players_collection.delete_one({"_id": player_id})
    
    if result.deleted_count > 0:
        print(f"Player with ID {player_id} successfully deleted.")
    else:
        print(f"Error: Player with ID {player_id} not found.")

        


        
