package com.isums.paymentservice.services;

import com.isums.houseservice.grpc.HouseResponse;
import com.isums.paymentservice.domains.dtos.finance.CategoryAmountDto;
import com.isums.paymentservice.domains.dtos.finance.FinanceDashboardDto;
import com.isums.paymentservice.domains.dtos.finance.FinanceSummaryDto;
import com.isums.paymentservice.domains.dtos.finance.MonthlyPointDto;
import com.isums.paymentservice.domains.dtos.finance.OutstandingInvoiceDto;
import com.isums.paymentservice.domains.dtos.finance.TopHouseStatDto;
import com.isums.paymentservice.domains.dtos.finance.TransactionDto;
import com.isums.paymentservice.domains.dtos.finance.projections.HouseAggregateProjection;
import com.isums.paymentservice.domains.dtos.finance.projections.MonthlyTotalProjection;
import com.isums.paymentservice.domains.dtos.finance.projections.TypeAmountProjection;
import com.isums.paymentservice.domains.entities.RentalInvoice;
import com.isums.paymentservice.domains.enums.InvoiceType;
import com.isums.paymentservice.infrastructures.grpcs.HouseGrpcClient;
import com.isums.paymentservice.infrastructures.grpcs.UserGrpcService;
import com.isums.paymentservice.infrastructures.repositories.RentalInvoiceRepository;
import com.isums.userservice.grpc.UserResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.time.LocalDate;
import java.time.YearMonth;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
@Slf4j
public class FinanceDashboardService {

    private static final ZoneId VN_ZONE = ZoneId.of("Asia/Ho_Chi_Minh");

    private static final List<String> REVENUE_TYPES = List.of(
            InvoiceType.MONTHLY_RENT.name(),
            InvoiceType.PENALTY.name());

    private static final List<String> EXPENSE_TYPES = List.of(
            InvoiceType.MAINTENANCE.name(),
            InvoiceType.ISSUE.name(),
            InvoiceType.DEPOSIT_REFUND.name());

    private static final int TOP_HOUSE_LIMIT = 10;
    private static final int RECENT_TX_LIMIT = 15;
    private static final int OUTSTANDING_LIMIT = 50;

    private final RentalInvoiceRepository invoiceRepository;
    private final HouseGrpcClient houseGrpcClient;
    private final UserGrpcService userGrpcService;

    @Transactional(readOnly = true)
    @Cacheable(
            value = "finance-dashboard",
            key = "#keycloakId + ':' + #fromIso + ':' + #toIso + ':' + #compare",
            unless = "#result == null"
    )
    public FinanceDashboardDto getDashboard(
            String keycloakId,
            String fromIso,
            String toIso,
            boolean compare) {
        Instant from = parseInstant(fromIso);
        Instant to = parseInstant(toIso);
        if (!from.isBefore(to)) {
            throw new IllegalArgumentException("from must be before to");
        }

        UserResponse user = userGrpcService.getUserIdAndRoleByKeyCloakId(keycloakId);
        if (user == null || user.getId() == null || user.getId().isBlank()) {
            throw new IllegalStateException("Failed to resolve actor: " + keycloakId);
        }
        UUID internalUserId = UUID.fromString(user.getId());
        boolean landlord = isLandlord(user);

        ScopedHouses scoped = resolveScopedHouses(landlord, internalUserId);

        FinanceSummaryDto summary = buildSummary(scoped, from, to, compare);
        List<CategoryAmountDto> revenueBreakdown = buildBreakdown(scoped, from, to, REVENUE_TYPES);
        List<CategoryAmountDto> expenseBreakdown = buildBreakdown(scoped, from, to, EXPENSE_TYPES);
        List<MonthlyPointDto> monthlyTrend = buildMonthlyTrend(scoped, from, to);
        List<TopHouseStatDto> topHouses = buildTopHouses(scoped, from, to);
        List<TransactionDto> recentTransactions = buildRecentTransactions(scoped, from, to);
        List<OutstandingInvoiceDto> outstandingInvoices = buildOutstanding(scoped);

        Instant[] previous = previousPeriod(from, to);

        return FinanceDashboardDto.builder()
                .periodFrom(from)
                .periodTo(to)
                .previousPeriodFrom(compare ? previous[0] : null)
                .previousPeriodTo(compare ? previous[1] : null)
                .summary(summary)
                .revenueBreakdown(revenueBreakdown)
                .expenseBreakdown(expenseBreakdown)
                .monthlyTrend(monthlyTrend)
                .topHouses(topHouses)
                .recentTransactions(recentTransactions)
                .outstandingInvoices(outstandingInvoices)
                .totalManagedHouses(scoped.totalHouses())
                .build();
    }

    private FinanceSummaryDto buildSummary(
            ScopedHouses scoped,
            Instant from,
            Instant to,
            boolean compare) {
        long revenue = sumByTypes(scoped, from, to, REVENUE_TYPES);
        long expense = sumByTypes(scoped, from, to, EXPENSE_TYPES);
        long netProfit = revenue - expense;

        Instant now = Instant.now();
        long outstandingAmount = nz(invoiceRepository.sumOutstandingAmount(
                now, scoped.scoped(), scoped.houseIds()));
        long outstandingCount = invoiceRepository.countOutstanding(
                now, scoped.scoped(), scoped.houseIds());

        Long previousRevenue = null;
        Long previousExpense = null;
        Long previousProfit = null;
        Double revenuePct = null;
        Double expensePct = null;
        Double profitPct = null;
        if (compare) {
            Instant[] prev = previousPeriod(from, to);
            previousRevenue = sumByTypes(scoped, prev[0], prev[1], REVENUE_TYPES);
            previousExpense = sumByTypes(scoped, prev[0], prev[1], EXPENSE_TYPES);
            previousProfit = previousRevenue - previousExpense;
            revenuePct = changePercent(previousRevenue, revenue);
            expensePct = changePercent(previousExpense, expense);
            profitPct = changePercent(previousProfit, netProfit);
        }

        return FinanceSummaryDto.builder()
                .totalRevenue(revenue)
                .totalExpense(expense)
                .netProfit(netProfit)
                .outstandingAmount(outstandingAmount)
                .outstandingCount((int) Math.min(outstandingCount, Integer.MAX_VALUE))
                .previousRevenue(previousRevenue)
                .previousExpense(previousExpense)
                .previousNetProfit(previousProfit)
                .revenueChangePercent(revenuePct)
                .expenseChangePercent(expensePct)
                .netProfitChangePercent(profitPct)
                .build();
    }

    private List<CategoryAmountDto> buildBreakdown(
            ScopedHouses scoped,
            Instant from,
            Instant to,
            List<String> types) {
        List<TypeAmountProjection> rows = invoiceRepository.aggregatePaidByType(
                types, from, to, scoped.scoped(), scoped.houseIds());
        long total = rows.stream().mapToLong(r -> nz(r.getAmount())).sum();
        Map<String, Long> byType = new HashMap<>();
        for (TypeAmountProjection row : rows) {
            byType.put(row.getType(), nz(row.getAmount()));
        }
        List<CategoryAmountDto> result = new ArrayList<>(types.size());
        for (String type : types) {
            long amount = byType.getOrDefault(type, 0L);
            double percent = total == 0L ? 0.0 : ((double) amount / (double) total) * 100.0;
            result.add(CategoryAmountDto.builder()
                    .category(type)
                    .amount(amount)
                    .percent(round1(percent))
                    .build());
        }
        return result;
    }

    private List<MonthlyPointDto> buildMonthlyTrend(
            ScopedHouses scoped,
            Instant from,
            Instant to) {
        List<String> allTypes = new ArrayList<>(REVENUE_TYPES.size() + EXPENSE_TYPES.size());
        allTypes.addAll(REVENUE_TYPES);
        allTypes.addAll(EXPENSE_TYPES);
        List<MonthlyTotalProjection> rows = invoiceRepository.aggregateMonthlyByType(
                allTypes, from, to, scoped.scoped(), scoped.houseIds());

        Set<String> revenueTypes = new java.util.HashSet<>(REVENUE_TYPES);
        Map<String, long[]> bucket = new LinkedHashMap<>();

        YearMonth start = YearMonth.from(from.atZone(VN_ZONE));
        YearMonth end = YearMonth.from(to.minusSeconds(1).atZone(VN_ZONE));
        YearMonth cursor = start;
        while (!cursor.isAfter(end)) {
            bucket.put(cursor.toString(), new long[2]);
            cursor = cursor.plusMonths(1);
        }

        for (MonthlyTotalProjection row : rows) {
            long[] arr = bucket.computeIfAbsent(row.getMonth(), k -> new long[2]);
            long amount = nz(row.getAmount());
            if (revenueTypes.contains(row.getType())) {
                arr[0] += amount;
            } else {
                arr[1] += amount;
            }
        }

        List<MonthlyPointDto> result = new ArrayList<>(bucket.size());
        for (Map.Entry<String, long[]> entry : bucket.entrySet()) {
            long revenue = entry.getValue()[0];
            long expense = entry.getValue()[1];
            result.add(MonthlyPointDto.builder()
                    .month(entry.getKey())
                    .revenue(revenue)
                    .expense(expense)
                    .profit(revenue - expense)
                    .build());
        }
        return result;
    }

    private List<TopHouseStatDto> buildTopHouses(
            ScopedHouses scoped,
            Instant from,
            Instant to) {
        List<HouseAggregateProjection> rows = invoiceRepository.aggregateByHouse(
                from, to, scoped.scoped(), scoped.houseIds(),
                PageRequest.of(0, TOP_HOUSE_LIMIT));
        if (rows.isEmpty()) {
            return List.of();
        }
        Map<UUID, HouseResponse> houseMap = scoped.houseMap();
        List<TopHouseStatDto> result = new ArrayList<>(rows.size());
        for (HouseAggregateProjection row : rows) {
            UUID houseId = row.getHouseId();
            HouseResponse house = houseMap.get(houseId);
            if (house == null) {
                house = houseGrpcClient.getHouse(houseId);
            }
            String name = house != null ? safe(house.getName()) : null;
            String address = house != null ? safe(house.getAddress()) : null;
            long revenue = nz(row.getRevenue());
            long expense = nz(row.getExpense());
            result.add(TopHouseStatDto.builder()
                    .houseId(houseId)
                    .houseName(name)
                    .address(address)
                    .revenue(revenue)
                    .expense(expense)
                    .profit(revenue - expense)
                    .build());
        }
        return result;
    }

    private List<TransactionDto> buildRecentTransactions(
            ScopedHouses scoped,
            Instant from,
            Instant to) {
        List<RentalInvoice> rows = invoiceRepository.findRecentPaid(
                from, to, scoped.scoped(), scoped.houseIds(),
                PageRequest.of(0, RECENT_TX_LIMIT));
        Map<UUID, HouseResponse> houseMap = scoped.houseMap();
        return rows.stream()
                .map(invoice -> TransactionDto.builder()
                        .invoiceId(invoice.getId())
                        .contractId(invoice.getContractId())
                        .houseId(invoice.getHouseId())
                        .houseName(resolveHouseName(invoice.getHouseId(), houseMap))
                        .type(invoice.getType())
                        .status(invoice.getStatus())
                        .amount(nz(invoice.getTotalAmount()))
                        .paidAt(invoice.getPaidAt())
                        .dueDate(invoice.getDueDate())
                        .tenantEmail(invoice.getTenantEmail())
                        .build())
                .toList();
    }

    private List<OutstandingInvoiceDto> buildOutstanding(ScopedHouses scoped) {
        Instant now = Instant.now();
        List<RentalInvoice> rows = invoiceRepository.findOutstanding(
                now, scoped.scoped(), scoped.houseIds(),
                PageRequest.of(0, OUTSTANDING_LIMIT));
        Map<UUID, HouseResponse> houseMap = scoped.houseMap();
        return rows.stream()
                .map(invoice -> OutstandingInvoiceDto.builder()
                        .invoiceId(invoice.getId())
                        .contractId(invoice.getContractId())
                        .houseId(invoice.getHouseId())
                        .houseName(resolveHouseName(invoice.getHouseId(), houseMap))
                        .type(invoice.getType())
                        .amount(nz(invoice.getTotalAmount()))
                        .dueDate(invoice.getDueDate())
                        .daysOverdue(daysBetween(invoice.getDueDate(), now))
                        .tenantEmail(invoice.getTenantEmail())
                        .build())
                .toList();
    }

    private long sumByTypes(ScopedHouses scoped, Instant from, Instant to, List<String> types) {
        return invoiceRepository.aggregatePaidByType(types, from, to, scoped.scoped(), scoped.houseIds())
                .stream()
                .mapToLong(row -> nz(row.getAmount()))
                .sum();
    }

    private ScopedHouses resolveScopedHouses(boolean landlord, UUID internalUserId) {
        if (landlord) {
            List<HouseResponse> houses = houseGrpcClient.getAllHouses();
            return ScopedHouses.unscoped(houses);
        }
        List<HouseResponse> houses = houseGrpcClient.getHousesByManagerRegion(internalUserId);
        return ScopedHouses.scoped(houses);
    }

    private static boolean isLandlord(UserResponse user) {
        if (user.getRolesList() == null) {
            return false;
        }
        return user.getRolesList().stream()
                .anyMatch(role -> "LANDLORD".equalsIgnoreCase(role));
    }

    private static String resolveHouseName(UUID houseId, Map<UUID, HouseResponse> houseMap) {
        HouseResponse house = houseMap.get(houseId);
        return house == null ? null : safe(house.getName());
    }

    private static String safe(String value) {
        return value == null || value.isBlank() ? null : value;
    }

    private static long nz(Long value) {
        return value == null ? 0L : value;
    }

    private static long daysBetween(Instant from, Instant to) {
        if (from == null || to == null) return 0L;
        return Math.max(0L, ChronoUnit.DAYS.between(from, to));
    }

    private static Double changePercent(Long previous, long current) {
        if (previous == null || previous == 0L) {
            if (current == 0L) return 0.0;
            return null;
        }
        double ratio = ((double) (current - previous) / Math.abs((double) previous)) * 100.0;
        return round1(ratio);
    }

    private static double round1(double v) {
        return Math.round(v * 10.0) / 10.0;
    }

    private static Instant[] previousPeriod(Instant from, Instant to) {
        long durationMillis = to.toEpochMilli() - from.toEpochMilli();
        Instant prevTo = from;
        Instant prevFrom = from.minusMillis(durationMillis);
        return new Instant[]{prevFrom, prevTo};
    }

    private static Instant parseInstant(String iso) {
        if (iso == null || iso.isBlank()) {
            throw new IllegalArgumentException("Date is required");
        }
        try {
            return Instant.parse(iso);
        } catch (Exception e) {
            try {
                LocalDate date = LocalDate.parse(iso);
                return date.atStartOfDay(VN_ZONE).toInstant();
            } catch (Exception inner) {
                throw new IllegalArgumentException("Invalid ISO date: " + iso);
            }
        }
    }

    private record ScopedHouses(boolean scoped, List<UUID> houseIds, Map<UUID, HouseResponse> houseMap, long totalHouses) {

        static ScopedHouses unscoped(List<HouseResponse> houses) {
            Map<UUID, HouseResponse> map = toHouseMap(houses);
            List<UUID> ids = new ArrayList<>(map.keySet());
            if (ids.isEmpty()) {
                ids = List.of(new UUID(0L, 0L));
            }
            return new ScopedHouses(false, ids, map, map.size());
        }

        static ScopedHouses scoped(List<HouseResponse> houses) {
            Map<UUID, HouseResponse> map = toHouseMap(houses);
            List<UUID> ids = new ArrayList<>(map.keySet());
            if (ids.isEmpty()) {
                ids = List.of(new UUID(0L, 0L));
            }
            return new ScopedHouses(true, ids, map, map.size());
        }

        private static Map<UUID, HouseResponse> toHouseMap(List<HouseResponse> houses) {
            if (houses == null || houses.isEmpty()) return Collections.emptyMap();
            return houses.stream()
                    .filter(h -> h.getId() != null && !h.getId().isBlank())
                    .collect(Collectors.toMap(
                            h -> UUID.fromString(h.getId()),
                            h -> h,
                            (a, b) -> a));
        }
    }
}
