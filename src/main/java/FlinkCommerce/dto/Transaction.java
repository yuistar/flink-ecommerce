package FlinkCommerce.dto;

import lombok.Data;
import java.io.Serializable;
import java.sql.Timestamp;

@Data
public class Transaction implements Serializable {
    private String transactionId;
    private String productId;
    private String productName;
    private String productCategory;
    private double productPrice;
    private int productQuantity;
    private String productBrand;
    private double totalAmount;
    private String currency;
    private String customerId;
    private Timestamp transactionDate;
    private String paymentMethod;
}
