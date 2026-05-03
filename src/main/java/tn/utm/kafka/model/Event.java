package tn.utm.kafka.model;

import java.util.List;

public class Event {

    public String type;
    public String idCaisse;
    public String ville;
    public String timestamp;
    public Double montant;
    public List<String> produits;

    public Event() {}
}