{% extends "base.html" %}
{% block content %}
<h1>Lotes / fardos</h1>
<section class="panel">
  <h2>Crear lote</h2>
  <form method="post" action="/lotes" class="form-grid">
    <label>Nombre lote
      <input name="name" placeholder="Fardo vestuario mujer premium" required>
    </label>
    <label>Familia
      <select name="family"><option value="">General</option>{% for f in families %}<option value="{{ f }}">{{ f|capitalize }}</option>{% endfor %}</select>
    </label>
    <label>Peso inicial kg
      <input name="initial_weight_kg" placeholder="45.0">
    </label>
    <label>Costo lote
      <input name="cost_amount" placeholder="180000">
    </label>
    <button class="primary" type="submit">Crear lote</button>
  </form>
</section>
<section class="panel">
  <h2>Lotes registrados</h2>
  <table>
    <thead><tr><th>ID</th><th>Fecha</th><th>Nombre</th><th>Familia</th><th>Peso inicial</th><th>Costo</th><th>Estado</th><th></th></tr></thead>
    <tbody>
    {% for lot in lots %}
      <tr><td>{{ lot.id }}</td><td>{{ lot.created_at.strftime('%d-%m-%Y') }}</td><td>{{ lot.name }}</td><td>{{ lot.family or '--' }}</td><td>{% if lot.initial_weight_kg %}{{ '%.2f'|format(lot.initial_weight_kg) }} kg{% else %}--{% endif %}</td><td>{% if lot.cost_amount %}{{ format_clp(lot.cost_amount) }}{% else %}--{% endif %}</td><td>{{ lot.status }}</td><td>{% if lot.status == 'abierto' %}<form method="post" action="/lotes/{{ lot.id }}/cerrar"><button type="submit">Cerrar</button></form>{% endif %}</td></tr>
    {% else %}<tr><td colspan="8" class="muted">Sin lotes.</td></tr>{% endfor %}
    </tbody>
  </table>
</section>
{% endblock %}
