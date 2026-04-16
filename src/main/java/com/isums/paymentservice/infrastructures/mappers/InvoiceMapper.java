package com.isums.paymentservice.infrastructures.mappers;

import com.isums.paymentservice.domains.dtos.InvoiceDto;
import com.isums.paymentservice.domains.entities.RentalInvoice;
import org.mapstruct.Mapper;

import java.util.List;

@Mapper(componentModel = "spring")
public interface InvoiceMapper {
    InvoiceDto toDto(RentalInvoice invoice);
    List<InvoiceDto> toDtos(List<RentalInvoice> invoices);
}
