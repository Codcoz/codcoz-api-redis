from flask import Flask, request, jsonify
import redis
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

# Carregando variáveis de ambiente
load_dotenv()
REDIS_URL = os.getenv("REDIS_URL")

# Conexão com o redis
r = redis.Redis.from_url(REDIS_URL, decode_responses=True)

# Constantes do redis
TEMPO_TTL = 86400
CHAVE_HISTORICO_BAIXAS = "historico_baixas"
CHAVE_CONFIG_HISTORICO_BAIXAS = "config_historico_baixas"

def rd_buscar_configs(empresa_id) -> list:
    result_set = r.hgetall(f"{CHAVE_CONFIG_HISTORICO_BAIXAS}:{str(empresa_id)}")
    return result_set

def rd_setar_configs(dias: int, empresa_id) -> bool:
    result_set = r.hset(f"{CHAVE_CONFIG_HISTORICO_BAIXAS}:{str(empresa_id)}", mapping={"dias": str(dias)})
    return result_set

def rd_excluir_registro(historico_id, empresa_id) -> bool:
    num_chaves_excluidas = r.delete(f"{CHAVE_HISTORICO_BAIXAS}:{str(empresa_id)}:{str(historico_id)}")

    return True if num_chaves_excluidas > 0 else False

def rd_excluir_todos_registros() -> bool:
    try:
        return r.flushall()  # True se tiver deletado tudo
    except Exception as e:
        print(f"Erro ao limpar todas as chaves do Redis: {e}")
        return False

def rd_setar_registro(item_historico: dict, empresa_id) -> int:
    # Incrementando o "id" serialmente
    historico_id = r.incr(f"{CHAVE_HISTORICO_BAIXAS}:counter")
    item_historico["id"] = str(historico_id)
    num_campos_adicionados = r.hset(f"{CHAVE_HISTORICO_BAIXAS}:{str(empresa_id)}:{historico_id}", mapping=item_historico)

    # Setando o tempo de expiração de uma chave
    dias_ttl = r.hget(f"{CHAVE_CONFIG_HISTORICO_BAIXAS}:{str(empresa_id)}", "dias")
    if dias_ttl == None:
        dias_ttl = 30
    else:
        dias_ttl = int(dias_ttl)
    
    r.expire(f"{CHAVE_HISTORICO_BAIXAS}:{historico_id}", dias_ttl * TEMPO_TTL)
    
    return historico_id if num_campos_adicionados > 0 else 0

def rd_buscar_registro_por_id(historico_id, empresa_id) -> dict:  
    return r.hgetall(f"{CHAVE_HISTORICO_BAIXAS}:{str(empresa_id)}:{str(historico_id)}")

def rd_buscar_todos_registros(empresa_id) -> list:
    keys = r.keys(f"{CHAVE_HISTORICO_BAIXAS}:{str(empresa_id)}*")
    resultados = []

    for k in keys:
        if k != f"{CHAVE_HISTORICO_BAIXAS}:{str(empresa_id)}:counter":
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

@app.route("/api/v1/empresa/<empresa_id>/historico_baixas/config", methods=["POST"])
def set_config_value(empresa_id) -> bool:
    data = request.get_json()

    dias_expiracao = data.get("dias_expiracao")

    rd_setar_configs(dias=dias_expiracao, empresa_id=empresa_id)

    return jsonify({"message": f"Configuração alterada para a empresa com o id: {empresa_id}"})

    
@app.route("/api/v1/empresa/<empresa_id>/historico_baixas/config", methods=["GET"])
def get_configs(empresa_id):
    result_set = rd_buscar_configs(empresa_id)
    
    if result_set:
        return jsonify({"configs": result_set})
    else:
        return jsonify({"error": f"Não foi possível pegar os registros da empresa com o id: {empresa_id}"})

@app.route("/api/v1/empresa/<empresa_id>/historico_baixas", methods=["POST"])
def set_value(empresa_id):
    data = request.get_json()

    id_produto = data.get("id_produto")
    nome_produto = data.get("nome_produto")
    codigo_produto = data.get("codigo_produto")
    data_acontecimento = data.get("data_acontecimento")
    tipo_registro = data.get("tipo_registro")

    if not tipo_registro:
        return jsonify({"error": "É necessário fornecer o campo 'tipo_registro'"}), 400
    if not id_produto:
        return jsonify({"error": "É necessário fornecer o campo 'id_produto'"}), 400
    if not nome_produto:
        return jsonify({"error": "É necessário fornecer o campo 'nome_produto'"}), 400
    if not codigo_produto:
        return jsonify({"error": "É necessário fornecer o campo 'codigo_produto'"}), 400
    if not data_acontecimento:
        return jsonify({"error": "É necessário fornecer o campo 'data_acontecimento'"}), 400

    result_set = rd_setar_registro(item_historico=data, empresa_id=empresa_id) 

    if result_set > 0:
        return jsonify({"message": f"Registro com id: {result_set} armazenado com sucesso!"}), 200
    else:
        return jsonify({"error": f"Há algo de errado nos parâmetros do body."}), 500

@app.route("/api/v1/empresa/<empresa_id>/historico_baixas/<historico_id>", methods=["DELETE"])
def delete_value_by_id(empresa_id, historico_id):
    result_del = rd_excluir_registro(empresa_id=empresa_id, historico_id=historico_id)
    if result_del:
        return jsonify({"message": f"Registro com id: {historico_id} excluído com sucesso!"}), 200
    else:
        return jsonify({"error": f"Registro com id: {historico_id} não encontrada."}), 404

@app.route("/api/v1/historico_baixas", methods=["DELETE"])
def delete_values():
    result_del = rd_excluir_todos_registros()
    if result_del:
        return jsonify({"message": f"Todos os registros excluídos!"})
    else:
        return jsonify({"error": f"Algum erro aconteceu e não foi possível excluir todos os registros."}), 404

@app.route("/api/v1/empresa/<empresa_id>/historico_baixas/<historico_id>", methods=["GET"])
def get_value_by_id(empresa_id, historico_id):
    result = rd_buscar_registro_por_id(empresa_id=empresa_id, historico_id=historico_id)
    if len(result.values()) <= 0:
        return jsonify({"error": f"Registro com id: {historico_id} não encontrado."}), 404
    return jsonify({"baixa": result}   )

@app.route("/api/v1/empresa/<empresa_id>/historico_baixas/leitura", methods=["POST"])
def get_values(empresa_id):
    data = request.get_json()
    
    tipo_ordenacao = data.get("tipo_ordenacao")
    tipo_registro = data.get("tipo_registro")
    periodo = data.get("periodo")

    # Pegando todos os registros existentes
    resultados = rd_buscar_todos_registros(empresa_id=empresa_id)

    return jsonify({"historico_baixas": rd_filtrar_registros(dados=resultados, tipo_registro=tipo_registro, periodo=periodo, tipo_ordenacao=tipo_ordenacao)})

@app.route("/health", methods=["GET"])
def health_check():
    return jsonify({
        "status": "ok",
        "timestamp": datetime.now().isoformat()
    })

if __name__ == "__main__":
    app.run(debug=True)