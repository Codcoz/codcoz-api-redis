from flask import Flask, request, jsonify
import redis
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv

app = Flask(__name__)

# Carregando variáveis de ambiente
load_dotenv()
REDIS_URL = os.getenv("REDIS_URL")

# Conexão com o redis
r = redis.Redis.from_url(REDIS_URL, decode_responses=True)

# Constantes do redis
TEMPO_TTL = 86400
CHAVE_HISTORICO_BAIXAS = "historico_baixas"
CHAVE_CONFIG_HISTORICO_BAIXAS = "config_historico_baixas"

def rd_setar_tempo_ttl(dias: int) -> bool:
    result_set = r.set(f"{CHAVE_CONFIG_HISTORICO_BAIXAS}:dias_ttl", str(dias))
    return result_set

def rd_excluir_registro(id) -> bool:
    num_campos_excluidos = r.hdel(f"{CHAVE_HISTORICO_BAIXAS}:{str(id)}")

    return True if num_campos_excluidos > 0 else False

def rd_setar_registro(item_historico: dict) -> int:
    # Incrementando o "id" serialmente
    id = r.incr(f"{CHAVE_HISTORICO_BAIXAS}:counter")
    item_historico["id"] = str(id)
    num_campos_adicionados = r.hset(f"{CHAVE_HISTORICO_BAIXAS}:{id}", mapping=item_historico)

    # Setando o tempo de expiração de uma chave
    dias_ttl = r.get(f"{CHAVE_CONFIG_HISTORICO_BAIXAS}:dias_ttl")
    if dias_ttl == None:
        dias_ttl = 30
    else:
        dias_ttl = int(dias_ttl)
    
    r.expire(f"{CHAVE_HISTORICO_BAIXAS}:{id}", dias_ttl * TEMPO_TTL)
    
    return id if num_campos_adicionados > 0 else 0

def rd_buscar_registro_por_id(id) -> dict:  
    return r.hgetall(f"{CHAVE_HISTORICO_BAIXAS}:{str(id)}")

def rd_buscar_todos_registros() -> list:
    keys = r.keys(f"{CHAVE_HISTORICO_BAIXAS}:*")
    resultados = []

    for k in keys:
        if k != f"{CHAVE_HISTORICO_BAIXAS}:counter":
            value = r.hgetall(k)
            resultados.append(value)

    return resultados

def rd_filtrar_registros(dados: list, tipo_registro: str, periodo: str, tipo_ordenacao: str) -> list:
    filtrados = dados

    # Filtro por tipo
    if tipo_registro:
        filtrados = [d for d in filtrados if d["tipo_registro"] == tipo_registro]

    # Filtro por período
    if periodo:
        hoje = datetime.now().date()
        if periodo == "hoje":
            filtrados = [d for d in filtrados if d["data_acontecimento"].date() == hoje]
        elif periodo == "ontem":
            ontem = hoje - timedelta(days=1)
            filtrados = [d for d in filtrados if d["data_acontecimento"].date() == ontem]
        elif periodo.startswith("ultimos"):
            dias = int(periodo.split(" ")[1])  # valores possíveis: "ultimos 7", "ultimos 15", "ultimos 30"
            limite = hoje - timedelta(days=dias)
            filtrados = [d for d in filtrados if d["data_acontecimento"].date() >= limite]

    # Ordenação
    if tipo_ordenacao:
        filtrados = sorted(
            filtrados,
            key=lambda d: d["data_acontecimento"],
            reverse=(tipo_ordenacao == "desc")
        )

    return filtrados


@app.route("/config/set", methods=["POST"])
def set_config_value() -> bool:
    data = request.get_json()

    dias_expiracao = data.get("dias_expiracao")

    result_set = rd_setar_tempo_ttl(dias_expiracao)

    return result_set

@app.route("/set", methods=["POST"])
def set_value():
    data = request.get_json()

    id_produto = data.get("id_produto")
    nome_produto = data.get("nome_produto")
    codigo_produto = data.get("codigo_produto")
    data_acontecimento = data.get("data_acontecimento")
    tipo_registro = data.get("tipo_registro")

    if not tipo_registro:
        return jsonify({"error": "É necessário fornecer o campo 'tipo'"}), 400
    if not id_produto:
        return jsonify({"error": "É necessário fornecer o campo 'id_produto'"}), 400
    if not nome_produto:
        return jsonify({"error": "É necessário fornecer o campo 'nome_produto'"}), 400
    if not codigo_produto:
        return jsonify({"error": "É necessário fornecer o campo 'codigo_produto'"}), 400
    if not data_acontecimento:
        return jsonify({"error": "É necessário fornecer o campo 'data_acontecimento'"}), 400

    result_set = rd_setar_registro(data) 

    if result_set > 0:
        return jsonify({"message": f"Registro com id: {result_set} armazenado com sucesso!"})
    else:
        return jsonify({"error": f"Há algo de errado nos parâmetros do body."})

@app.route("/delete/key", methods=["DELETE"])
def delete_value_by_id(key):
    result_del = rd_excluir_registro(key)
    if result_del:
        return jsonify({"message": f"Registro com id: {key} excluído com sucesso!"})
    else:
        return jsonify({"error": f"Registro com id: {key} não encontrada"}), 404

@app.route("/get/<key>", methods=["GET"])
def get_value_by_id(key):
    result = rd_buscar_registro_por_id(key)
    if result is None:
        return jsonify({"error": f"Registro com id: {key} não encontrada"}), 404
    return jsonify({id: result})

@app.route("/get", methods=["GET"])
def get_values():
    data = request.get_json()
    
    tipo_ordenacao = data.get("tipo_ordenacao")
    tipo_registro = data.get("tipo_registro")
    periodo = data.get("periodo")

    # Pegando todos os registros existentes
    resultados = rd_buscar_todos_registros()

    return jsonify({"historico_baixas": rd_filtrar_registros(dados=resultados, tipo_registro=tipo_registro, periodo=periodo, tipo_ordenacao=tipo_ordenacao)})

if __name__ == "__main__":
    app.run(debug=True)