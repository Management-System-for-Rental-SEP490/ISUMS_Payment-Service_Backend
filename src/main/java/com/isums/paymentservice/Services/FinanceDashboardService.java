package com.isums.paymentservice.services;

import com.isums.houseservice.grpc.HouseResponse;
import com.isums.paymentservice.domains.dtos.finance.CategoryAmountDto;
import com.isums.paymentservice.domains.dtos.finance.FinanceDashboardDto;
import com.isums.paymentservice.domains.dtos.finance.FinanceSummaryDto;
import com.isums.paymentservice.domains.dtos.finance.MonthlyPointDto;
import com.isums.paymentservice.domains.dtos.finance.OutstandingInvoiceDto;
import com.isums.paymentservice.domains.dtos.finance.RegionFinanceSummaryDto;
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
import java.util.Comparator;
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
            InvoiceType.UTILITY.name(),
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
            key = "#keycloakId + ':' + #fromIso + ':' + #toIso + ':' + #compare + ':' + (#regionId == null ? 'ALL' : #regionId)",
            unless = "#result == null"
    )
    public FinanceDashboardDto getDashboard(
            String keycloakId,
            String fromIso,
            String toIso,
            boolean compare,
            UUID regionId) {
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

        FinanceScope scope = resolveFinanceScope(user, internalUserId, regionId);

        FinanceSummaryDto summary = buildSummary(scope, from, to, compare);
        List<CategoryAmountDto> revenueBreakdown = buildBreakdown(scope, from, to, REVENUE_TYPES);
        List<CategoryAmountDto> expenseBreakdown = buildBreakdown(scope, from, to, EXPENSE_TYPES);
        List<RegionFinanceSummaryDto> regionSummaries = buildRegionSummaries(scope, from, to);
        List<MonthlyPointDto> monthlyTrend = buildMonthlyTrend(scope, from, to);
        List<TopHouseStatDto> topHouses = buildTopHouses(scope, from, to);
        List<TransactionDto> recentTransactions = buildRecentTransactions(scope, from, to);
        List<OutstandingInvoiceDto> outstandingInvoices = buildOutstanding(scope);

        Instant[] previous = previousPeriod(from, to);

        return FinanceDashboardDto.builder()
                .periodFrom(from)
                .periodTo(to)
                .previousPeriodFrom(compare ? previous[0] : null)
                .previousPeriodTo(compare ? previous[1] : null)
                .summary(summary)
                .revenueBreakdown(revenueBreakdown)
                .expenseBreakdown(expenseBreakdown)
                .regionSummaries(regionSummaries)
                .monthlyTrend(monthlyTrend)
                .topHouses(topHouses)
                .recentTransactions(recentTransactions)
                .outstandingInvoices(outstandingInvoices)
                .totalManagedHouses(scope.totalHouses())
                .build();
    }

    private FinanceSummaryDto buildSummary(
            FinanceScope scope,
            Instant from,
            Instant to,
            boolean compare) {
        long revenue = sumByTypes(scope, from, to, REVENUE_TYPES);
        long expense = sumByTypes(scope, from, to, EXPENSE_TYPES);
        long netProfit = revenue - expense;

        Instant now = Instant.now();
        long outstandingAmount = nz(invoiceRepository.sumOutstandingAmount(
                now, scope.applyHouseFilter(), scope.queryHouseIds()));
        long outstandingCount = invoiceRepository.countOutstanding(
                now, scope.applyHouseFilter(), scope.queryHouseIds());

        Long previousRevenue = null;
        Long previousExpense = null;
        Long previousProfit = null;
        Double revenuePct = null;
        Double expensePct = null;
        Double profitPct = null;
        if (compare) {
            Instant[] prev = previousPeriod(from, to);
            previousRevenue = sumByTypes(scope, prev[0], prev[1], REVENUE_TYPES);
            previousExpense = sumByTypes(scope, prev[0], prev[1], EXPENSE_TYPES);
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
            FinanceScope scope,
            Instant from,
            Instant to,
            List<String> types) {
        List<TypeAmountProjection> rows = invoiceRepository.aggregatePaidByType(
                types, from, to, scope.applyHouseFilter(), scope.queryHouseIds());
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
            FinanceScope scope,
            Instant from,
            Instant to) {
        List<String> allTypes = new ArrayList<>(REVENUE_TYPES.size() + EXPENSE_TYPES.size());
        allTypes.addAll(REVENUE_TYPES);
        allTypes.addAll(EXPENSE_TYPES);
        List<MonthlyTotalProjection> rows = invoiceRepository.aggregateMonthlyByType(
                allTypes, from, to, scope.applyHouseFilter(), scope.queryHouseIds());

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
            FinanceScope scope,
            Instant from,
            Instant to) {
        List<HouseAggregateProjection> rows = invoiceRepository.aggregateByHouse(
                from, to, scope.applyHouseFilter(), scope.queryHouseIds(),
                PageRequest.of(0, TOP_HOUSE_LIMIT));
        if (rows.isEmpty()) {
            return List.of();
        }
        Map<UUID, HouseResponse> houseMap = scope.houseMap();
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
            FinanceScope scope,
            Instant from,
            Instant to) {
        List<RentalInvoice> rows = invoiceRepository.findRecentPaid(
                from, to, scope.applyHouseFilter(), scope.queryHouseIds(),
                PageRequest.of(0, RECENT_TX_LIMIT));
        Map<UUID, HouseResponse> houseMap = scope.houseMap();
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

    private List<OutstandingInvoiceDto> buildOutstanding(FinanceScope scope) {
        Instant now = Instant.now();
        List<RentalInvoice> rows = invoiceRepository.findOutstanding(
                now, scope.applyHouseFilter(), scope.queryHouseIds(),
                PageRequest.of(0, OUTSTANDING_LIMIT));
        Map<UUID, HouseResponse> houseMap = scope.houseMap();
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

    private List<RegionFinanceSummaryDto> buildRegionSummaries(FinanceScope scope, Instant from, Instant to) {
        if (scope.houseMap().isEmpty()) {
            return List.of();
        }
        Map<UUID, List<UUID>> housesByRegion = scope.houseMap().entrySet().stream()
                .filter(entry -> entry.getValue().getRegionId() != null && !entry.getValue().getRegionId().isBlank())
                .collect(Collectors.groupingBy(
                        entry -> UUID.fromString(entry.getValue().getRegionId()),
                        LinkedHashMap::new,
                        Collectors.mapping(Map.Entry::getKey, Collectors.toList())));

        Instant now = Instant.now();
        return housesByRegion.entrySet().stream()
                .sorted(Comparator.comparing(entry -> entry.getKey().toString()))
                .map(entry -> {
                    FinanceScope regionScope = FinanceScope.forHouses(
                            scope.type(),
                            scope.requestedRegionId(),
                            entry.getValue(),
                            filterHouseMap(scope.houseMap(), entry.getValue()));
                    long revenue = sumByTypes(regionScope, from, to, REVENUE_TYPES);
                    long expense = sumByTypes(regionScope, from, to, EXPENSE_TYPES);
                    long outstandingAmount = nz(invoiceRepository.sumOutstandingAmount(
                            now, regionScope.applyHouseFilter(), regionScope.queryHouseIds()));
                    long outstandingCount = invoiceRepository.countOutstanding(
                            now, regionScope.applyHouseFilter(), regionScope.queryHouseIds());
                    return RegionFinanceSummaryDto.builder()
                            .regionId(entry.getKey())
                            .totalHouses(entry.getValue().size())
                            .totalRevenue(revenue)
                            .totalExpense(expense)
                            .netProfit(revenue - expense)
                            .outstandingAmount(outstandingAmount)
                            .outstandingCount((int) Math.min(outstandingCount, Integer.MAX_VALUE))
                            .build();
                })
                .toList();
    }

    private long sumByTypes(FinanceScope scope, Instant from, Instant to, List<String> types) {
        return invoiceRepository.aggregatePaidByType(types, from, to, scope.applyHouseFilter(), scope.queryHouseIds())
                .stream()
                .mapToLong(row -> nz(row.getAmount()))
                .sum();
    }

    private FinanceScope resolveFinanceScope(UserResponse user, UUID internalUserId, UUID requestedRegionId) {
        FinanceScopeType type;
        if (hasRole(user, "LANDLORD")) {
            type = FinanceScopeType.GLOBAL;
        } else if (hasRole(user, "MANAGER")) {
            type = FinanceScopeType.MANAGED_REGIONS;
        } else {
            throw new IllegalStateException("Actor is not allowed to view finance dashboard");
        }

        List<HouseResponse> houses;
        boolean globalUnfiltered = type == FinanceScopeType.GLOBAL && requestedRegionId == null;
        if (type == FinanceScopeType.GLOBAL) {
            houses = houseGrpcClient.getAllHouses();
            return FinanceScope.fromHouses(type, requestedRegionId, houses, globalUnfiltered);
        }

        houses = houseGrpcClient.getHousesByManagerRegion(internalUserId);
        return FinanceScope.fromHouses(type, requestedRegionId, houses, false);
    }

    private static boolean hasRole(UserResponse user, String roleName) {
        if (user.getRolesList() == null) {
            return false;
        }
        return user.getRolesList().stream()
                .anyMatch(role -> roleName.equalsIgnoreCase(role));
    }

    private static Map<UUID, HouseResponse> filterHouseMap(Map<UUID, HouseResponse> source, List<UUID> ids) {
        Set<UUID> idSet = Set.copyOf(ids);
        return source.entrySet().stream()
                .filter(entry -> idSet.contains(entry.getKey()))
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue,
                        (a, b) -> a,
                        LinkedHashMap::new));
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

    private enum FinanceScopeType {
        GLOBAL,
        MANAGED_REGIONS
    }

    private record FinanceScope(
            FinanceScopeType type,
            UUID requestedRegionId,
            boolean applyHouseFilter,
            List<UUID> queryHouseIds,
            Map<UUID, HouseResponse> houseMap,
            long totalHouses) {

        static FinanceScope fromHouses(
                FinanceScopeType type,
                UUID requestedRegionId,
                List<HouseResponse> houses,
                boolean globalUnfiltered) {
            Map<UUID, HouseResponse> map = toHouseMap(houses);
            if (requestedRegionId != null) {
                map = map.entrySet().stream()
                        .filter(entry -> requestedRegionId.toString().equals(entry.getValue().getRegionId()))
                        .collect(Collectors.toMap(
                                Map.Entry::getKey,
                                Map.Entry::getValue,
                                (a, b) -> a,
                                LinkedHashMap::new));
            }

            List<UUID> ids = queryIds(map);
            return new FinanceScope(type, requestedRegionId, !globalUnfiltered, ids, map, map.size());
        }

        static FinanceScope forHouses(
                FinanceScopeType type,
                UUID requestedRegionId,
                List<UUID> houseIds,
                Map<UUID, HouseResponse> houseMap) {
            return new FinanceScope(type, requestedRegionId, true, queryIds(houseIds), houseMap, houseMap.size());
        }

        private static List<UUID> queryIds(Map<UUID, HouseResponse> map) {
            return queryIds(new ArrayList<>(map.keySet()));
        }

        private static List<UUID> queryIds(List<UUID> ids) {
            if (ids.isEmpty()) {
                return List.of(new UUID(0L, 0L));
            }
            return ids;
        }

        private static Map<UUID, HouseResponse> toHouseMap(List<HouseResponse> houses) {
            if (houses == null || houses.isEmpty()) return Collections.emptyMap();
            return houses.stream()
                    .filter(h -> h.getId() != null && !h.getId().isBlank())
                    .collect(Collectors.toMap(
                            h -> UUID.fromString(h.getId()),
                            h -> h,
                            (a, b) -> a,
                            LinkedHashMap::new));
        }
    }
}
