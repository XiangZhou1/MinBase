package org.minbase.client.admin;

import org.minbase.client.service.AdminService;
import org.minbase.common.rpc.proto.generated.AdminProto;

import java.util.ArrayList;
import java.util.List;

public class ClientAdmin implements Admin {
    AdminService adminService;

    public ClientAdmin(AdminService adminService) {
        this.adminService = adminService;
    }

    @Override
    public boolean createTable(String tableName, List<String> columns) {
        AdminProto.CreateTableRequest.Builder builder = AdminProto.CreateTableRequest.newBuilder();
        builder.setTableName(tableName).addAllColumns(columns);
        AdminProto.CreateTableResponse createTableResponse = adminService.createTable(builder.build());
        return createTableResponse.getSuccess();
    }

    @Override
    public boolean addColumn(String tableName, List<String> columns) {
        AdminProto.AddColumnRequest.Builder builder = AdminProto.AddColumnRequest.newBuilder();
        builder.setTableName(tableName).addAllColumns(columns);
        AdminProto.AddColumnResponse addColumnResponse = adminService.addColumn(builder.build());
        return addColumnResponse.getSuccess();
    }

    @Override
    public boolean dropTable(String tableName) {
        AdminProto.DropTableRequest.Builder builder = AdminProto.DropTableRequest.newBuilder();
        builder.setTableName(tableName);
        AdminProto.DropTableResponse dropTableResponse = adminService.dropTable(builder.build());
        return dropTableResponse.getSuccess();
    }

    @Override
    public List<String> getTableInfo(String tableName) {
        AdminProto.GetTableInfoRequest.Builder builder = AdminProto.GetTableInfoRequest.newBuilder();
        builder.setTableName(tableName);
        AdminProto.GetTableInfoResponse getTableInfoResponse = adminService.getTableInfo(builder.build());
        List<String> columns = new ArrayList<>();
        for (int i = 0; i < getTableInfoResponse.getColumnsCount(); i++) {
            columns.add(getTableInfoResponse.getColumns(i));
        }
        return columns;
    }
}
