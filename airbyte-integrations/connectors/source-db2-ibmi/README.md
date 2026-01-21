# IBM i DB2 Source

## Documentation

- [User Documentation](https://docs.airbyte.io/integrations/sources/db2-ibmi)

This connector uses the IBM JTOpen driver (https://github.com/IBM/JTOpen) to connect to IBM i (formerly AS/400) DB2 databases.

## Key Features

- **JTOpen Driver**: Uses the open-source JTOpen JDBC driver specifically designed for IBM i systems
- **IBM i Compatibility**: Properly handles IBM i system schemas (QSYS, QSYS2) and catalog views
- **SSL/TLS Support**: Supports secure connections using the `secure=true` property
- **Flexible Configuration**: Supports custom JDBC URL parameters for fine-tuned connections

## Configuration

- **Default Port**: 8471 (DRDA/DDM protocol for IBM i database connections)
- **SSL Port**: 992 (for secure connections)
- **Database**: Use `*SYSBAS` for system database or specify a specific library/schema

## Integration tests

For acceptance tests run

`./gradlew :airbyte-integrations:connectors:source-db2-ibmi:integrationTest`
