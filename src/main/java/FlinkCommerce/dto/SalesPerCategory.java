package FlinkCommerce.dto;

import lombok.Data;

import java.sql.Date;

@Data
public class SalesPerCategory {
    private Date transactionDate;
    private String category;
    private double totalSales;

    public SalesPerCategory(Date transactionDate, String category, double totalSales) {
        this.transactionDate = transactionDate;
        this.category = category;
        this.totalSales = totalSales;
    }
}
