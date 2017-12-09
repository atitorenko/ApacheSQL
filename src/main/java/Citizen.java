import lombok.*;
import org.apache.ignite.cache.query.annotations.QuerySqlField;

import java.io.Serializable;

/**
 * Model of storing and querieng entity.
 */
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@ToString
@EqualsAndHashCode
public class Citizen implements Serializable {
    private static final long serialVersionUID = 1L;
    /**
     * id.
     */
    @QuerySqlField(index =  true)
    long id;
    /**
     * number of passport.
     */
    @QuerySqlField
    int passportNumber;

    /**
     * Moth (1-12) related to index.
     */
    @QuerySqlField
    int month;

    /**
     * May be as abroad trips per month as salary per given month.
     */
    @QuerySqlField
    int index;
}
